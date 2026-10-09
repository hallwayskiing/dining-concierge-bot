"""Dry-run deploy.py against moto. Lex/OpenSearch calls are validated against the real
botocore API models (parameter names, types, required fields) and answered with fakes."""
import json
import os
import sys
import tempfile
import time
import types

os.environ.update(AWS_DEFAULT_REGION="us-east-1", AWS_ACCESS_KEY_ID="x", AWS_SECRET_ACCESS_KEY="x", MOTO_IAM_LOAD_MANAGED_POLICIES="true")
from moto import mock_aws  # noqa: E402
import boto3  # noqa: E402
from botocore.validate import validate_parameters  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)


class Validating:
    """Validate every call against the service model, then return a canned response."""
    def __init__(self, service, responses):
        self.client = boto3.client(service, region_name="us-east-1")
        self.model = self.client.meta.service_model
        self.responses, self.calls = responses, []

    def get_waiter(self, name):
        assert name in self.client.waiter_names, name
        return types.SimpleNamespace(wait=lambda **kw: None)

    def __getattr__(self, name):
        op = self.client.meta.method_to_api_mapping[name]
        shape = self.model.operation_model(op).input_shape

        def call(**params):
            params = {k: v for k, v in params.items() if k != "WaiterConfig"}
            validate_parameters(params, shape)
            self.calls.append((name, params))
            r = self.responses.get(name, {})
            return r(params) if callable(r) else r
        return call


slot_n = iter(range(100))
lex_fake = Validating("lexv2-models", {
    "list_bots": {"botSummaries": []},
    "create_bot": {"botId": "BOTID12345"},
    "create_intent": {"intentId": "INTENTID01"},
    "create_slot": lambda p: {"slotId": f"SLOTID{next(slot_n):04d}"},
    "create_bot_version": {"botVersion": "1"},
    "create_bot_alias": {"botAliasId": "ALIASID123"},
})

with mock_aws():
    import deploy
    deploy.STATE_FILE = os.path.join(tempfile.mkdtemp(), "state.json")
    deploy.lex = lex_fake
    # moto only imports OpenAPI 3; emulate Swagger-2 import with create_rest_api (real AWS supports 2.0).
    real_api = deploy.apigw

    class ApiShim:
        def __getattr__(self, n):
            return getattr(real_api, n)

        def _build(self, api_id, body):
            spec = json.loads(body)
            assert spec["swagger"] == "2.0" and "options" in spec["paths"]["/chatbot"]
            root = real_api.get_resources(restApiId=api_id)["items"][0]["id"]
            rid = real_api.create_resource(restApiId=api_id, parentId=root, pathPart="chatbot")["id"]
            for m in ("POST", "OPTIONS"):
                real_api.put_method(restApiId=api_id, resourceId=rid, httpMethod=m, authorizationType="NONE")
                real_api.put_integration(restApiId=api_id, resourceId=rid, httpMethod=m, type="MOCK")

        def import_rest_api(self, body, **kw):
            api = real_api.create_rest_api(name=json.loads(body)["info"]["title"])
            self._build(api["id"], body)
            return api

        def put_rest_api(self, restApiId, body, **kw):
            return {"id": restApiId}
    deploy.apigw = ApiShim()
    real_sleep = time.sleep
    deploy.time.sleep = lambda s: None
    args = types.SimpleNamespace(email="me@nyu.edu")
    deploy.cmd_core(args)
    st = deploy.load_state()

    # API: check the imported swagger got integrations + CORS
    api = boto3.client("apigateway")
    res = {r["path"]: r for r in api.get_resources(restApiId=st["api_id"])["items"]}
    print("API resources:", {p: sorted((r.get("resourceMethods") or {}).keys()) for p, r in res.items()})
    swagger = json.loads(deploy.build_swagger("arn:aws:lambda:us-east-1:123:function:LF0"))
    assert swagger["paths"]["/chatbot"]["post"]["x-amazon-apigateway-integration"]["type"] == "aws_proxy"
    # frontend uploaded with the real API url injected
    js = boto3.client("s3").get_object(Bucket=st["bucket"], Key="assets/js/sdk/apigClient.js")["Body"].read().decode()
    assert st["api_url"] in js and "abc123" not in js
    print("Lambda envs:", {f: boto3.client("lambda").get_function_configuration(FunctionName=f)["Environment"]["Variables"]
                           for f in ("LF0", "LF1")})
    print("Lex calls:", [c[0] for c in lex_fake.calls])
    rule = boto3.client("events").describe_rule(Name=deploy.RULE_NAME)
    print("Rule:", rule["ScheduleExpression"], rule["State"])

    # Re-run core: must be idempotent
    deploy.lex.responses["list_bots"] = {"botSummaries": [{"botName": deploy.BOT_NAME, "botId": "BOTID12345"}]}
    deploy.cmd_core(args)

    # OpenSearch create_domain params validated against the model
    es_fake = Validating("opensearch", {
        "list_versions": {"Versions": ["OpenSearch_2.19", "OpenSearch_2.9", "Elasticsearch_7.10", "OpenSearch_1.3"]},
        "describe_domain": lambda p: {"DomainStatus": {"Endpoint": "search-x.us-east-1.es.amazonaws.com",
                                                       "Processing": False, "Created": True}},
    })
    first = {"n": 0}

    def describe(p):
        first["n"] += 1
        if first["n"] == 1:
            from botocore.exceptions import ClientError
            raise ClientError({"Error": {"Code": "ResourceNotFoundException"}}, "DescribeDomain")
        return {"DomainStatus": {"Endpoint": "search-x.us-east-1.es.amazonaws.com", "Processing": False, "Created": True}}
    es_fake.responses["describe_domain"] = describe
    deploy.es = es_fake
    print("engine picked:", deploy.latest_engine())
    import opensearch_load
    opensearch_load.load = lambda *a, **k: print("  (fake) load", a[0])
    deploy.cmd_opensearch(types.SimpleNamespace())
    print("LF2 env:", boto3.client("lambda").get_function_configuration(FunctionName="LF2")["Environment"]["Variables"])
    print("Rule now:", boto3.client("events").describe_rule(Name=deploy.RULE_NAME)["State"])
    print("\nDRY RUN OK")
