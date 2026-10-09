"""Create the OpenSearch index "restaurants" and load RestaurantID + Cuisine for every
restaurant in DynamoDB ("yelp-restaurants"). Each document is of type "Restaurant".

Usage:  python3 scripts/opensearch_load.py <endpoint> <user> <password>
"""
import base64
import json
import sys
import urllib.error
import urllib.request

import boto3

INDEX = "restaurants"
DOC_TYPE = "Restaurant"   # OpenSearch removed mapping types; we record the type as a field instead.

MAPPING = {
    "settings": {"number_of_shards": 1, "number_of_replicas": 0},
    "mappings": {
        "properties": {
            "RestaurantID": {"type": "keyword"},
            "Cuisine": {"type": "keyword"},
            "type": {"type": "keyword"},
        }
    },
}


class OS:
    def __init__(self, endpoint, user, password):
        self.endpoint = endpoint.rstrip("/")
        if not self.endpoint.startswith("http"):
            self.endpoint = "https://" + self.endpoint
        self.auth = "Basic " + base64.b64encode(f"{user}:{password}".encode()).decode()

    def call(self, method, path, body=None, ndjson=False):
        data = None
        if body is not None:
            data = body.encode() if ndjson else json.dumps(body).encode()
        req = urllib.request.Request(self.endpoint + path, data=data, method=method)
        req.add_header("Authorization", self.auth)
        req.add_header("Content-Type", "application/x-ndjson" if ndjson else "application/json")
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                return resp.status, json.loads(resp.read() or b"{}")
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read() or b"{}")


def load(endpoint, user, password, table_name="yelp-restaurants", region=None):
    client = OS(endpoint, user, password)
    status, _ = client.call("GET", f"/{INDEX}")
    if status == 200:
        client.call("DELETE", f"/{INDEX}")
    status, body = client.call("PUT", f"/{INDEX}", MAPPING)
    if status >= 300:
        raise RuntimeError(f"Could not create index: {status} {body}")

    table = boto3.resource("dynamodb", region_name=region).Table(table_name)
    items, kwargs = [], {"ProjectionExpression": "BusinessID, Cuisine"}
    while True:
        page = table.scan(**kwargs)
        items += page["Items"]
        if "LastEvaluatedKey" not in page:
            break
        kwargs["ExclusiveStartKey"] = page["LastEvaluatedKey"]

    for start in range(0, len(items), 500):
        lines = []
        for it in items[start:start + 500]:
            lines.append(json.dumps({"index": {"_index": INDEX, "_id": it["BusinessID"]}}))
            lines.append(json.dumps({"RestaurantID": it["BusinessID"], "Cuisine": it["Cuisine"], "type": DOC_TYPE}))
        status, body = client.call("POST", "/_bulk", "\n".join(lines) + "\n", ndjson=True)
        if status >= 300 or body.get("errors"):
            raise RuntimeError(f"Bulk load failed: {status} {str(body)[:500]}")
    client.call("POST", f"/{INDEX}/_refresh")
    _, count = client.call("GET", f"/{INDEX}/_count")
    print(f"Indexed {count.get('count')} restaurants into OpenSearch index '{INDEX}'.")
    return count.get("count")


if __name__ == "__main__":
    load(*sys.argv[1:4])
