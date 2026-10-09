#!/usr/bin/env python3
"""One-command deployment for the Dining Concierge chatbot (Cloud Computing HW1).

Run inside AWS CloudShell (us-east-1):

    python3 deploy.py core --email you@example.com      # everything except OpenSearch (~5 min)
    python3 deploy.py scrape --yelp-key YOUR_YELP_KEY    # 1,000+ restaurants -> DynamoDB
    python3 deploy.py opensearch                         # domain + index + load (~20 min)
    python3 deploy.py test --email you@example.com       # scripted chat through the live API
    python3 deploy.py status                             # print URLs / ids
    python3 deploy.py logs LF1                           # recent Lambda logs (debugging)
    python3 deploy.py delete-opensearch                  # stop OpenSearch billing
    python3 deploy.py destroy                            # remove everything

Every step is idempotent: if something fails, fix it and re-run the same command.
"""
import argparse
import io
import json
import mimetypes
import os
import secrets
import string
import sys
import time
import urllib.request
import uuid
import zipfile

import boto3
from botocore.exceptions import ClientError

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "scripts"))

REGION = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "us-east-1"
STATE_FILE = os.path.join(HERE, ".deploy_state.json")

RESTAURANT_TABLE = "yelp-restaurants"
STATE_TABLE = "dining-user-state"
QUEUE_NAME, DLQ_NAME = "Q1", "Q1-dlq"
BOT_NAME, LOCALE = "DiningConciergeBot", "en_US"
API_NAME = "AI Customer Service API"
OS_DOMAIN, OS_USER = "restaurants-domain", "concierge-admin"
RULE_NAME = "dining-lf2-every-minute"
LAMBDAS = {"LF0": "lf0", "LF1": "lf1", "LF2": "lf2"}

session = boto3.session.Session(region_name=REGION)
iam, lam, sqs = session.client("iam"), session.client("lambda"), session.client("sqs")
ddb, s3, ses = session.client("dynamodb"), session.client("s3"), session.client("ses")
lex, lexrt = session.client("lexv2-models"), session.client("lexv2-runtime")
apigw, events, es = session.client("apigateway"), session.client("events"), session.client("opensearch")
ACCOUNT = None


# ====================================================================== utils
def log(msg):
    print(f"\033[1;36m▶\033[0m {msg}", flush=True)


def ok(msg):
    print(f"  \033[32m✓\033[0m {msg}", flush=True)


def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return json.load(f)
    return {}


def save_state(st):
    with open(STATE_FILE, "w") as f:
        json.dump(st, f, indent=2)


def code(e):
    return e.response["Error"]["Code"]


def account_id():
    global ACCOUNT
    if not ACCOUNT:
        ACCOUNT = session.client("sts").get_caller_identity()["Account"]
    return ACCOUNT


def retry(fn, tries=12, delay=5, retry_on=("InvalidParameterValueException", "AccessDeniedException",
                                          "ValidationException", "ConflictException",
                                          "PreconditionFailedException", "ResourceConflictException")):
    for i in range(tries):
        try:
            return fn()
        except ClientError as e:
            if code(e) in retry_on and i < tries - 1:
                time.sleep(delay)
                continue
            raise


def msg(text):
    return {"messageGroups": [{"message": {"plainTextMessage": {"value": text}}}], "maxRetries": 2,
            "allowInterrupt": True}


# ====================================================================== IAM
def ensure_role(name, service, managed=(), inline=None):
    trust = {"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Principal": {"Service": service}, "Action": "sts:AssumeRole"}]}
    try:
        arn = iam.create_role(RoleName=name, AssumeRolePolicyDocument=json.dumps(trust))["Role"]["Arn"]
        new = True
    except ClientError as e:
        if code(e) != "EntityAlreadyExists":
            raise
        arn, new = iam.get_role(RoleName=name)["Role"]["Arn"], False
    for p in managed:
        iam.attach_role_policy(RoleName=name, PolicyArn=p)
    if inline:
        iam.put_role_policy(RoleName=name, PolicyName=f"{name}-inline", PolicyDocument=json.dumps(
            {"Version": "2012-10-17", "Statement": inline}))
    if new:
        time.sleep(10)  # IAM propagation
    ok(f"role {name}")
    return arn


def roles(st):
    log("IAM roles")
    a, r = account_id(), REGION
    basic = ["arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"]
    q1 = f"arn:aws:sqs:{r}:{a}:{QUEUE_NAME}"
    state_arn = f"arn:aws:dynamodb:{r}:{a}:table/{STATE_TABLE}"
    rest_arn = f"arn:aws:dynamodb:{r}:{a}:table/{RESTAURANT_TABLE}"
    st["roles"] = {
        "LF0": ensure_role("dining-lf0-role", "lambda.amazonaws.com", basic, [
            {"Effect": "Allow", "Action": ["lex:RecognizeText"], "Resource": f"arn:aws:lex:{r}:{a}:bot-alias/*"}]),
        "LF1": ensure_role("dining-lf1-role", "lambda.amazonaws.com", basic, [
            {"Effect": "Allow", "Action": ["sqs:SendMessage"], "Resource": q1},
            {"Effect": "Allow", "Action": ["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:UpdateItem"],
             "Resource": state_arn}]),
        "LF2": ensure_role("dining-lf2-role", "lambda.amazonaws.com", basic, [
            {"Effect": "Allow", "Action": ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes"],
             "Resource": q1},
            {"Effect": "Allow", "Action": ["dynamodb:BatchGetItem", "dynamodb:GetItem"], "Resource": rest_arn},
            {"Effect": "Allow", "Action": ["dynamodb:GetItem", "dynamodb:UpdateItem"], "Resource": state_arn},
            {"Effect": "Allow", "Action": ["ses:SendEmail", "ses:SendRawEmail"], "Resource": "*"}]),
        "LEX": ensure_role("dining-lex-role", "lexv2.amazonaws.com", (), [
            {"Effect": "Allow", "Action": ["polly:SynthesizeSpeech", "comprehend:DetectSentiment"],
             "Resource": "*"}]),
    }
    save_state(st)


# ====================================================================== DynamoDB / SQS / SES
def ensure_table(name, key):
    try:
        ddb.create_table(TableName=name, BillingMode="PAY_PER_REQUEST",
                         AttributeDefinitions=[{"AttributeName": key, "AttributeType": "S"}],
                         KeySchema=[{"AttributeName": key, "KeyType": "HASH"}])
    except ClientError as e:
        if code(e) != "ResourceInUseException":
            raise
    ddb.get_waiter("table_exists").wait(TableName=name)
    ok(f"DynamoDB table {name}")


def tables(st):
    log("DynamoDB tables")
    ensure_table(RESTAURANT_TABLE, "BusinessID")
    ensure_table(STATE_TABLE, "userId")


def queues(st):
    log("SQS queue Q1 (+ dead-letter queue)")
    dlq_url = sqs.create_queue(QueueName=DLQ_NAME)["QueueUrl"]
    dlq_arn = sqs.get_queue_attributes(QueueUrl=dlq_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    attrs = {"VisibilityTimeout": "120",
             "RedrivePolicy": json.dumps({"deadLetterTargetArn": dlq_arn, "maxReceiveCount": "5"})}
    try:
        url = sqs.create_queue(QueueName=QUEUE_NAME, Attributes=attrs)["QueueUrl"]
    except ClientError as e:
        if code(e) != "QueueAlreadyExists":
            raise
        url = sqs.get_queue_url(QueueName=QUEUE_NAME)["QueueUrl"]
        sqs.set_queue_attributes(QueueUrl=url, Attributes=attrs)
    st["queue_url"], st["dlq_url"] = url, dlq_url
    save_state(st)
    ok(f"queue {url}")


def ses_verify(st, email):
    log("SES email identity")
    st["email"] = email
    save_state(st)
    status = ses.get_identity_verification_attributes(Identities=[email])["VerificationAttributes"] \
        .get(email, {}).get("VerificationStatus")
    if status == "Success":
        ok(f"{email} already verified")
        return
    ses.verify_email_identity(EmailAddress=email)
    ok(f"verification email sent to {email} — click the link in it (check spam).")


# ====================================================================== Lambda
def zip_dir(path):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for root, _, files in os.walk(path):
            for f in files:
                if f.endswith(".py"):
                    full = os.path.join(root, f)
                    z.write(full, os.path.relpath(full, path))
    return buf.getvalue()


def ensure_lambda(name, role_arn, env, timeout=30):
    zipped = zip_dir(os.path.join(HERE, "lambda", LAMBDAS[name]))
    cfg = dict(Role=role_arn, Handler="lambda_function.lambda_handler", Runtime="python3.12",
               Timeout=timeout, MemorySize=256, Environment={"Variables": env})
    try:
        lam.get_function(FunctionName=name)
        exists = True
    except ClientError as e:
        if code(e) != "ResourceNotFoundException":
            raise
        exists = False
    if exists:
        lam.update_function_code(FunctionName=name, ZipFile=zipped)
        lam.get_waiter("function_updated_v2").wait(FunctionName=name)
        retry(lambda: lam.update_function_configuration(FunctionName=name, **cfg))
        lam.get_waiter("function_updated_v2").wait(FunctionName=name)
    else:
        retry(lambda: lam.create_function(FunctionName=name, Code={"ZipFile": zipped}, **cfg))
        lam.get_waiter("function_active_v2").wait(FunctionName=name)
    arn = lam.get_function(FunctionName=name)["Configuration"]["FunctionArn"]
    ok(f"Lambda {name}")
    return arn


def allow_invoke(fn, sid, principal, source_arn):
    try:
        lam.remove_permission(FunctionName=fn, StatementId=sid)
    except ClientError:
        pass
    lam.add_permission(FunctionName=fn, StatementId=sid, Action="lambda:InvokeFunction",
                       Principal=principal, SourceArn=source_arn)


def update_env(fn, updates):
    current = lam.get_function_configuration(FunctionName=fn).get("Environment", {}).get("Variables", {})
    current.update(updates)
    retry(lambda: lam.update_function_configuration(FunctionName=fn, Environment={"Variables": current}))
    lam.get_waiter("function_updated_v2").wait(FunctionName=fn)


# ====================================================================== Lex V2
GREETING_UTTERANCES = ["hi", "hello", "hey", "hi there", "hello there", "hey there", "good morning",
                       "good afternoon", "good evening", "howdy", "what's up", "yo", "greetings"]
THANKS_UTTERANCES = ["thanks", "thank you", "thank you so much", "thanks a lot", "many thanks", "thx",
                     "appreciate it", "I appreciate it", "cheers", "great thanks", "awesome thank you",
                     "that's all thanks"]
DINING_UTTERANCES = ["I need some restaurant suggestions", "I need restaurant suggestions",
                     "restaurant suggestions", "dining suggestions", "suggest a restaurant",
                     "recommend a restaurant", "can you recommend a restaurant", "find me a restaurant",
                     "help me find a restaurant", "I'm hungry", "I am hungry", "I want to eat",
                     "where should I eat", "I want to go out to eat", "I'm looking for a place to eat",
                     "I'm looking for restaurant recommendations", "suggest somewhere to eat",
                     "I want to book a table", "find a place for dinner", "I need a place for lunch",
                     "recommend a place to eat", "what should I eat tonight", "get me restaurant ideas"]
SLOTS = [  # name, slot type, prompt, required?
    ("Location", "AMAZON.FreeFormInput",
     "Great. I can help you with that. What city or city area are you looking to dine in?", True),
    ("Cuisine", "AMAZON.FreeFormInput",
     "What cuisine would you like to try? I know Chinese, Japanese, Italian, Mexican, Indian, Thai and Korean.", True),
    ("NumberOfPeople", "AMAZON.Number", "Ok, how many people are in your party?", True),
    ("DiningDate", "AMAZON.Date", "A few more to go. What date?", True),
    ("DiningTime", "AMAZON.Time", "What time?", True),
    ("Email", "AMAZON.FreeFormInput",
     "Great. Lastly, I need your email address so I can send you my findings.", True),
    ("SameAsLast", "AMAZON.Confirmation",
     "Would you like the same recommendations as last time? (yes/no)", False),
]


def find_bot():
    for b in lex.list_bots(filters=[{"name": "BotName", "values": [BOT_NAME], "operator": "EQ"}])["botSummaries"]:
        if b["botName"] == BOT_NAME:
            return b["botId"]
    return None


def delete_bot(bot_id):
    lex.delete_bot(botId=bot_id, skipResourceInUseCheck=True)
    for _ in range(60):
        try:
            lex.describe_bot(botId=bot_id)
            time.sleep(5)
        except ClientError as e:
            if code(e) == "ResourceNotFoundException":
                return
            raise


def yes_no_slot_type(bot_id):
    return lex.create_slot_type(
        slotTypeName="YesNo", botId=bot_id, botVersion="DRAFT", localeId=LOCALE,
        slotTypeValues=[{"sampleValue": {"value": "Yes"}, "synonyms": [{"value": v} for v in
                                                                         ["yes", "yeah", "yep", "sure", "ok", "please do", "y"]]},
                        {"sampleValue": {"value": "No"}, "synonyms": [{"value": v} for v in
                                                                        ["no", "nope", "nah", "no thanks", "n"]]}],
        valueSelectionSetting={"resolutionStrategy": "TopResolution"})["slotTypeId"]


def lex_bot(st, lf1_arn):
    log("Amazon Lex V2 bot (takes ~3 min)")
    bot_id = find_bot()
    if bot_id and st.get("lex", {}).get("botId") == bot_id and st["lex"].get("aliasId"):
        ok(f"bot {BOT_NAME} exists ({bot_id}) — updating Lambda hook only")
    else:
        if bot_id:
            log("  removing half-built bot from a previous run")
            delete_bot(bot_id)
        bot_id = retry(lambda: lex.create_bot(
            botName=BOT_NAME, description="Dining Concierge chatbot (HW1)", roleArn=st["roles"]["LEX"],
            dataPrivacy={"childDirected": False}, idleSessionTTLInSeconds=600))["botId"]
        lex.get_waiter("bot_available").wait(botId=bot_id)
        lex.create_bot_locale(botId=bot_id, botVersion="DRAFT", localeId=LOCALE,
                              nluIntentConfidenceThreshold=0.40, voiceSettings={"voiceId": "Joanna"})
        lex.get_waiter("bot_locale_created").wait(botId=bot_id, botVersion="DRAFT", localeId=LOCALE)
        ok(f"bot {bot_id} + locale {LOCALE}")

        base = dict(botId=bot_id, botVersion="DRAFT", localeId=LOCALE)
        hooks = dict(dialogCodeHook={"enabled": True}, fulfillmentCodeHook={"enabled": True, "active": True})

        def utt(lst):
            return [{"utterance": u} for u in lst]

        for name, utterances in (("GreetingIntent", GREETING_UTTERANCES), ("ThankYouIntent", THANKS_UTTERANCES)):
            lex.create_intent(intentName=name, sampleUtterances=utt(utterances), **hooks, **base)
            ok(f"intent {name}")

        dining = lex.create_intent(intentName="DiningSuggestionsIntent", sampleUtterances=utt(DINING_UTTERANCES),
                                   **hooks, **base)["intentId"]
        priorities = []
        for i, (sname, stype, prompt, required) in enumerate(SLOTS, 1):
            kwargs = dict(slotName=sname, intentId=dining, slotTypeId=stype, valueElicitationSetting={
                "slotConstraint": "Required" if required else "Optional",
                "promptSpecification": msg(prompt)}, **base)
            try:
                sid = lex.create_slot(**kwargs)["slotId"]
            except ClientError as e:
                if stype != "AMAZON.Confirmation":
                    raise
                print(f"    AMAZON.Confirmation unavailable ({code(e)}); using custom YesNo slot type")
                kwargs["slotTypeId"] = yes_no_slot_type(bot_id)
                sid = lex.create_slot(**kwargs)["slotId"]
            priorities.append({"priority": i, "slotId": sid})
        lex.update_intent(intentId=dining, intentName="DiningSuggestionsIntent",
                          sampleUtterances=utt(DINING_UTTERANCES), slotPriorities=priorities, **hooks, **base)
        ok("intent DiningSuggestionsIntent (6 slots + SameAsLast)")

        lex.build_bot_locale(**base)
        lex.get_waiter("bot_locale_built").wait(WaiterConfig={"Delay": 10, "MaxAttempts": 60}, **base)
        ok("bot built")

        version = lex.create_bot_version(
            botId=bot_id, botVersionLocaleSpecification={LOCALE: {"sourceBotVersion": "DRAFT"}})["botVersion"]
        lex.get_waiter("bot_version_available").wait(botId=bot_id, botVersion=version,
                                                     WaiterConfig={"Delay": 10, "MaxAttempts": 60})
        st["lex"] = {"botId": bot_id, "version": version}
        save_state(st)

    a = account_id()
    allow_invoke("LF1", "lex-invoke", "lexv2.amazonaws.com", f"arn:aws:lex:{REGION}:{a}:bot-alias/{bot_id}/*")
    locale_settings = {LOCALE: {"enabled": True, "codeHookSpecification": {
        "lambdaCodeHook": {"lambdaARN": lf1_arn, "codeHookInterfaceVersion": "1.0"}}}}
    # Test alias (used by the Lex console "Test" panel) also gets the Lambda hook.
    retry(lambda: lex.update_bot_alias(botAliasId="TSTALIASID", botAliasName="TestBotAlias", botId=bot_id,
                                       botVersion="DRAFT", botAliasLocaleSettings=locale_settings))
    alias_id = st["lex"].get("aliasId")
    if alias_id:
        retry(lambda: lex.update_bot_alias(botAliasId=alias_id, botAliasName="prod", botId=bot_id,
                                           botVersion=st["lex"]["version"], botAliasLocaleSettings=locale_settings))
    else:
        alias_id = retry(lambda: lex.create_bot_alias(
            botAliasName="prod", botId=bot_id, botVersion=st["lex"]["version"],
            botAliasLocaleSettings=locale_settings))["botAliasId"]
    lex.get_waiter("bot_alias_available").wait(botId=bot_id, botAliasId=alias_id)
    st["lex"]["aliasId"] = alias_id
    save_state(st)
    ok(f"alias prod ({alias_id}) -> LF1 code hook")


# ====================================================================== API Gateway
CORS_HEADERS = "'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token'"


def build_swagger(lf0_arn):
    with open(os.path.join(HERE, "api", "swagger.json")) as f:
        spec = json.load(f)
    path = spec["paths"]["/chatbot"]
    path["post"]["x-amazon-apigateway-integration"] = {
        "type": "aws_proxy", "httpMethod": "POST", "passthroughBehavior": "when_no_match",
        "uri": f"arn:aws:apigateway:{REGION}:lambda:path/2015-03-31/functions/{lf0_arn}/invocations"}
    path["options"] = {
        "summary": "CORS support", "consumes": ["application/json"], "produces": ["application/json"],
        "responses": {"200": {"description": "CORS preflight", "headers": {
            "Access-Control-Allow-Origin": {"type": "string"}, "Access-Control-Allow-Methods": {"type": "string"},
            "Access-Control-Allow-Headers": {"type": "string"}}}},
        "x-amazon-apigateway-integration": {
            "type": "mock", "passthroughBehavior": "when_no_match",
            "requestTemplates": {"application/json": "{\"statusCode\": 200}"},
            "responses": {"default": {"statusCode": "200", "responseParameters": {
                "method.response.header.Access-Control-Allow-Methods": "'OPTIONS,POST'",
                "method.response.header.Access-Control-Allow-Headers": CORS_HEADERS,
                "method.response.header.Access-Control-Allow-Origin": "'*'"}}}}}
    cors = {"responseParameters": {"gatewayresponse.header.Access-Control-Allow-Origin": "'*'",
                                   "gatewayresponse.header.Access-Control-Allow-Headers": CORS_HEADERS}}
    spec["x-amazon-apigateway-gateway-responses"] = {"DEFAULT_4XX": cors, "DEFAULT_5XX": cors}
    return json.dumps(spec).encode()


def api_gateway(st, lf0_arn):
    log("API Gateway (imported from swagger.yaml, CORS enabled)")
    body = build_swagger(lf0_arn)
    api_id = st.get("api_id")
    if api_id:
        try:
            apigw.put_rest_api(restApiId=api_id, mode="overwrite", body=body, failOnWarnings=False)
        except ClientError as e:
            if code(e) != "NotFoundException":
                raise
            api_id = None
    if not api_id:
        api_id = apigw.import_rest_api(body=body, failOnWarnings=False,
                                       parameters={"endpointConfigurationTypes": "REGIONAL"})["id"]
    apigw.create_deployment(restApiId=api_id, stageName="v1", description="HW1 deployment")
    allow_invoke("LF0", "apigw-invoke", "apigateway.amazonaws.com",
                 f"arn:aws:execute-api:{REGION}:{account_id()}:{api_id}/*/POST/chatbot")
    st["api_id"] = api_id
    st["api_url"] = f"https://{api_id}.execute-api.{REGION}.amazonaws.com/v1"
    save_state(st)
    ok(f"API {st['api_url']}/chatbot")


# ====================================================================== S3 frontend
def frontend(st):
    log("Frontend -> S3 static website")
    bucket = st.get("bucket") or f"dining-concierge-{account_id()}-{REGION}"
    try:
        if REGION == "us-east-1":
            s3.create_bucket(Bucket=bucket)
        else:
            s3.create_bucket(Bucket=bucket, CreateBucketConfiguration={"LocationConstraint": REGION})
    except ClientError as e:
        if code(e) not in ("BucketAlreadyOwnedByYou",):
            raise
    try:
        s3.put_public_access_block(Bucket=bucket, PublicAccessBlockConfiguration={
            "BlockPublicAcls": True, "IgnorePublicAcls": True,
            "BlockPublicPolicy": False, "RestrictPublicBuckets": False})
        retry(lambda: s3.put_bucket_policy(Bucket=bucket, Policy=json.dumps({
            "Version": "2012-10-17", "Statement": [{"Sid": "PublicRead", "Effect": "Allow", "Principal": "*",
                                                    "Action": "s3:GetObject",
                                                    "Resource": f"arn:aws:s3:::{bucket}/*"}]})),
              retry_on=("AccessDenied", "MalformedPolicy"), tries=6)
    except ClientError as e:
        print(f"\n  !! Could not make the bucket public ({code(e)}). Your account probably has S3\n"
              f"     'Block Public Access' turned on at the account level. Turn it off in the S3\n"
              f"     console (Block Public Access settings for this account), then re-run.\n")
        raise
    s3.put_bucket_website(Bucket=bucket, WebsiteConfiguration={"IndexDocument": {"Suffix": "chat.html"},
                                                               "ErrorDocument": {"Key": "chat.html"}})
    root = os.path.join(HERE, "frontend")
    for dirpath, _, files in os.walk(root):
        for f in files:
            full = os.path.join(dirpath, f)
            key = os.path.relpath(full, root).replace(os.sep, "/")
            with open(full, "rb") as fh:
                data = fh.read()
            if key == "assets/js/sdk/apigClient.js":
                data = data.replace(b"https://abc123.execute-api.us-east-1.amazonaws.com/v1",
                                    st["api_url"].encode()).replace(b"'us-east-2'", f"'{REGION}'".encode())
            ctype = mimetypes.guess_type(f)[0] or "application/octet-stream"
            s3.put_object(Bucket=bucket, Key=key, Body=data, ContentType=ctype,
                          CacheControl="no-cache" if key.endswith((".html", ".js")) else "max-age=86400")
    st["bucket"] = bucket
    st["website_url"] = f"http://{bucket}.s3-website-{REGION}.amazonaws.com"
    save_state(st)
    ok(f"website {st['website_url']}")


# ====================================================================== EventBridge
def schedule(st, lf2_arn, enabled):
    rule_arn = events.put_rule(Name=RULE_NAME, ScheduleExpression="rate(1 minute)",
                               State="ENABLED" if enabled else "DISABLED",
                               Description="Invoke LF2 queue worker every minute")["RuleArn"]
    events.put_targets(Rule=RULE_NAME, Targets=[{"Id": "LF2", "Arn": lf2_arn}])
    allow_invoke("LF2", "events-invoke", "events.amazonaws.com", rule_arn)
    ok(f"EventBridge rule {RULE_NAME} ({'ENABLED' if enabled else 'disabled until OpenSearch is ready'})")


# ====================================================================== OpenSearch
def latest_engine():
    versions = es.list_versions(MaxResults=100)["Versions"]
    os_versions = [v for v in versions if v.startswith("OpenSearch_")]

    def key(v):
        return tuple(int(x) for x in v.split("_")[1].split(".") if x.isdigit())
    return max(os_versions, key=key)


def gen_password():
    alphabet = string.ascii_letters + string.digits
    core_pw = "".join(secrets.choice(alphabet) for _ in range(16))
    return "Aa1!" + core_pw


def opensearch(st):
    log("OpenSearch domain (1 x t3.small.search, no standby, 1 AZ) — creation takes 15-25 min")
    pw = st.get("os_password") or gen_password()
    st["os_password"] = pw
    save_state(st)
    try:
        es.describe_domain(DomainName=OS_DOMAIN)
        ok("domain already exists")
    except ClientError as e:
        if code(e) != "ResourceNotFoundException":
            raise
        engine = latest_engine()
        domain_arn = f"arn:aws:es:{REGION}:{account_id()}:domain/{OS_DOMAIN}/*"
        es.create_domain(
            DomainName=OS_DOMAIN, EngineVersion=engine,
            ClusterConfig={"InstanceType": "t3.small.search", "InstanceCount": 1, "DedicatedMasterEnabled": False,
                           "ZoneAwarenessEnabled": False, "MultiAZWithStandbyEnabled": False},
            EBSOptions={"EBSEnabled": True, "VolumeType": "gp3", "VolumeSize": 10},
            AccessPolicies=json.dumps({"Version": "2012-10-17", "Statement": [
                {"Effect": "Allow", "Principal": {"AWS": "*"}, "Action": "es:ESHttp*", "Resource": domain_arn}]}),
            NodeToNodeEncryptionOptions={"Enabled": True},
            EncryptionAtRestOptions={"Enabled": True},
            DomainEndpointOptions={"EnforceHTTPS": True},
            AdvancedSecurityOptions={"Enabled": True, "InternalUserDatabaseEnabled": True,
                                     "MasterUserOptions": {"MasterUserName": OS_USER, "MasterUserPassword": pw}})
        ok(f"creating {OS_DOMAIN} ({engine}); master user '{OS_USER}' (password saved in .deploy_state.json)")
    start = time.time()
    while True:
        d = es.describe_domain(DomainName=OS_DOMAIN)["DomainStatus"]
        if d.get("Endpoint") and not d.get("Processing") and d.get("Created"):
            break
        print(f"  ... waiting for domain ({int(time.time() - start) // 60} min)", flush=True)
        time.sleep(60)
    endpoint = "https://" + d["Endpoint"]
    st["os_endpoint"] = endpoint
    save_state(st)
    ok(f"domain ready: {endpoint}")

    import opensearch_load
    for attempt in range(10):  # fine-grained access control can take a few minutes to accept logins
        try:
            opensearch_load.load(endpoint, OS_USER, pw, RESTAURANT_TABLE, REGION)
            break
        except Exception as exc:
            if attempt == 9:
                raise
            print(f"  ... OpenSearch not accepting requests yet ({exc}); retrying in 30s", flush=True)
            time.sleep(30)

    update_env("LF2", {"OS_ENDPOINT": endpoint, "OS_USER": OS_USER, "OS_PASSWORD": pw})
    schedule(st, lam.get_function(FunctionName="LF2")["Configuration"]["FunctionArn"], enabled=True)
    ok("LF2 wired to OpenSearch; queue worker now runs every minute")


# ====================================================================== commands
def cmd_core(args):
    st = load_state()
    print(f"Deploying to account {account_id()} in {REGION}\n")
    ses_verify(st, args.email)
    roles(st)
    tables(st)
    queues(st)
    log("Lambda functions")
    lf1 = ensure_lambda("LF1", st["roles"]["LF1"], {"QUEUE_URL": st["queue_url"], "STATE_TABLE": STATE_TABLE})
    lf2_env = {"QUEUE_URL": st["queue_url"], "RESTAURANT_TABLE": RESTAURANT_TABLE, "STATE_TABLE": STATE_TABLE,
               "SENDER_EMAIL": args.email}
    if st.get("os_endpoint"):
        lf2_env.update({"OS_ENDPOINT": st["os_endpoint"], "OS_USER": OS_USER, "OS_PASSWORD": st["os_password"]})
    lf2 = ensure_lambda("LF2", st["roles"]["LF2"], lf2_env, timeout=120)
    lex_bot(st, lf1)
    lf0 = ensure_lambda("LF0", st["roles"]["LF0"], {"BOT_ID": st["lex"]["botId"],
                                                    "BOT_ALIAS_ID": st["lex"]["aliasId"], "LOCALE_ID": LOCALE})
    api_gateway(st, lf0)
    frontend(st)
    log("EventBridge schedule for LF2")
    schedule(st, lf2, enabled=bool(st.get("os_endpoint")))
    print("\nCore deployment done.")
    cmd_status(args)


def cmd_scrape(args):
    import yelp_scrape
    key = args.yelp_key or os.environ.get("YELP_API_KEY") or input("Yelp API key: ").strip()
    log("Scraping Yelp (Manhattan, 7 cuisines x ~200)")
    n = yelp_scrape.scrape(key, args.per_cuisine, RESTAURANT_TABLE, REGION,
                           backup=os.path.join(HERE, "data", "restaurants.json"))
    if n < 1000:
        print("  !! fewer than 1,000 restaurants; re-run scrape (it de-duplicates) or raise --per-cuisine")


def cmd_opensearch(args):
    st = load_state()
    count = ddb.describe_table(TableName=RESTAURANT_TABLE)["Table"].get("ItemCount", 0)
    print(f"(DynamoDB reports ~{count} restaurants; the count refreshes every ~6 h)")
    opensearch(st)


def post_chat(url, sid, text):
    body = json.dumps({"messages": [{"type": "unstructured", "unstructured": {"id": sid, "text": text}}]}).encode()
    req = urllib.request.Request(url + "/chatbot", data=body, method="POST",
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return [m["unstructured"]["text"] for m in json.loads(r.read())["messages"]]


def cmd_test(args):
    st = load_state()
    sid = args.session or "test-" + uuid.uuid4().hex[:8]
    script = ["Hello", "I need some restaurant suggestions.", "New Delhi", "Can you try Manhattan", "Japanese",
              "Two", "Tomorrow", "7 pm, please", args.email or st.get("email"), "Thank you!"]
    if args.repeat:  # extra credit: second search with the same location + cuisine
        script = ["Hi", "I need restaurant suggestions", "Manhattan", "Japanese", "yes", "Thanks"]
    print(f"Chatting with {st['api_url']}/chatbot as session {sid}\n")
    for line in script:
        print(f"\033[1mUser:\033[0m {line}")
        for reply in post_chat(st["api_url"], sid, line):
            print(f"\033[36mBot:\033[0m  {reply}")
    if args.run_worker:
        print("\nInvoking LF2 now instead of waiting for the schedule...")
        out = lam.invoke(FunctionName="LF2")["Payload"].read().decode()
        print(f"LF2 -> {out}  (check your inbox)")
    print(f"\nTo test the 'same as last time' extra credit: python3 deploy.py test --repeat --session {sid}")


def cmd_logs(args):
    """Print the last few minutes of a Lambda's CloudWatch logs (useful for debugging the bot)."""
    logs = session.client("logs")
    start = int((time.time() - args.minutes * 60) * 1000)
    kw = {"logGroupName": f"/aws/lambda/{args.function}", "startTime": start, "interleaved": True}
    try:
        while True:
            page = logs.filter_log_events(**kw)
            for e in page["events"]:
                line = e["message"].rstrip()
                if not line.startswith(("START", "END", "REPORT", "INIT_START")):
                    print(line)
            if "nextToken" not in page:
                break
            kw["nextToken"] = page["nextToken"]
    except ClientError as e:
        print(f"no logs yet for {args.function} ({code(e)})")


def cmd_status(args):
    st = load_state()
    print("\n" + "=" * 70)
    for label, key in [("Website (S3)", "website_url"), ("API", "api_url"), ("Q1", "queue_url"),
                       ("OpenSearch", "os_endpoint")]:
        print(f"  {label:<14} {st.get(key, '-')}")
    if st.get("lex"):
        print(f"  {'Lex bot':<14} {BOT_NAME} id={st['lex']['botId']} alias={st['lex'].get('aliasId')}")
    if st.get("email"):
        v = ses.get_identity_verification_attributes(Identities=[st["email"]])["VerificationAttributes"]
        print(f"  {'SES sender':<14} {st['email']} ({v.get(st['email'], {}).get('VerificationStatus', '?')})")
    print("=" * 70)


def cmd_delete_opensearch(args):
    st = load_state()
    log("Deleting OpenSearch domain (stops billing)")
    try:
        events.disable_rule(Name=RULE_NAME)
    except ClientError:
        pass
    try:
        es.delete_domain(DomainName=OS_DOMAIN)
        ok("deletion started (takes ~10 min in the background)")
    except ClientError as e:
        if code(e) != "ResourceNotFoundException":
            raise
        ok("no domain to delete")
    for k in ("os_endpoint",):
        st.pop(k, None)
    save_state(st)


def cmd_destroy(args):
    if input("Delete ALL Dining Concierge resources? type 'yes': ").strip() != "yes":
        return
    st = load_state()
    cmd_delete_opensearch(args)

    def quiet(fn, *a, **k):
        try:
            fn(*a, **k)
        except ClientError as e:
            print(f"    skip: {code(e)}")
    log("Removing everything else")
    quiet(events.remove_targets, Rule=RULE_NAME, Ids=["LF2"])
    quiet(events.delete_rule, Name=RULE_NAME)
    if st.get("api_id"):
        quiet(apigw.delete_rest_api, restApiId=st["api_id"])
    bot = find_bot()
    if bot:
        quiet(lex.delete_bot, botId=bot, skipResourceInUseCheck=True)
    for fn in LAMBDAS:
        quiet(lam.delete_function, FunctionName=fn)
    for q in ("queue_url", "dlq_url"):
        if st.get(q):
            quiet(sqs.delete_queue, QueueUrl=st[q])
    for t in (RESTAURANT_TABLE, STATE_TABLE):
        quiet(ddb.delete_table, TableName=t)
    if st.get("bucket"):
        b = session.resource("s3").Bucket(st["bucket"])
        try:
            b.objects.all().delete()
            b.delete()
        except ClientError as e:
            print(f"    skip bucket: {code(e)}")
    for role in ("dining-lf0-role", "dining-lf1-role", "dining-lf2-role", "dining-lex-role"):
        for p in iam.list_attached_role_policies(RoleName=role).get("AttachedPolicies", []) if _role_exists(role) else []:
            quiet(iam.detach_role_policy, RoleName=role, PolicyArn=p["PolicyArn"])
        quiet(iam.delete_role_policy, RoleName=role, PolicyName=f"{role}-inline")
        quiet(iam.delete_role, RoleName=role)
    os.remove(STATE_FILE) if os.path.exists(STATE_FILE) else None
    ok("done")


def _role_exists(name):
    try:
        iam.get_role(RoleName=name)
        return True
    except ClientError:
        return False


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("core"); p.add_argument("--email", required=True, help="SES sender (and test recipient)")
    p.set_defaults(fn=cmd_core)
    p = sub.add_parser("scrape"); p.add_argument("--yelp-key"); p.add_argument("--per-cuisine", type=int, default=200)
    p.set_defaults(fn=cmd_scrape)
    sub.add_parser("opensearch").set_defaults(fn=cmd_opensearch)
    p = sub.add_parser("test"); p.add_argument("--email"); p.add_argument("--session")
    p.add_argument("--repeat", action="store_true"); p.add_argument("--run-worker", action="store_true")
    p.set_defaults(fn=cmd_test)
    sub.add_parser("status").set_defaults(fn=cmd_status)
    p = sub.add_parser("logs"); p.add_argument("function", nargs="?", default="LF1", choices=list(LAMBDAS))
    p.add_argument("--minutes", type=int, default=30); p.set_defaults(fn=cmd_logs)
    sub.add_parser("delete-opensearch").set_defaults(fn=cmd_delete_opensearch)
    sub.add_parser("destroy").set_defaults(fn=cmd_destroy)
    args = ap.parse_args()
    os.makedirs(os.path.join(HERE, "data"), exist_ok=True)
    args.fn(args)


if __name__ == "__main__":
    main()
