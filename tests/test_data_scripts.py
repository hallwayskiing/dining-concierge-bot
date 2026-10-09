"""Offline tests for the Yelp scraper and the OpenSearch loader."""
import json
import os
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlparse

import boto3
from moto import mock_aws

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "scripts"))
os.environ.update(AWS_DEFAULT_REGION="us-east-1", AWS_ACCESS_KEY_ID="x", AWS_SECRET_ACCESS_KEY="x")
import yelp_scrape  # noqa: E402
import opensearch_load  # noqa: E402


def make_table():
    boto3.client("dynamodb").create_table(
        TableName="yelp-restaurants", BillingMode="PAY_PER_REQUEST",
        AttributeDefinitions=[{"AttributeName": "BusinessID", "AttributeType": "S"}],
        KeySchema=[{"AttributeName": "BusinessID", "KeyType": "HASH"}])


def test_scrape(monkeypatch, tmp_path):
    calls = []

    def fake_search(key, term, location, offset, limit=50):
        calls.append((term, location, offset))
        assert offset + limit <= 240
        cuisine = term.split()[0]
        # every area returns 120 businesses; ids overlap across areas and some across cuisines
        if offset >= 120:
            return []
        out = []
        for i in range(offset, min(offset + limit, 120)):
            bid = f"{cuisine}-{location[:3]}-{i}" if i % 10 else f"shared-{i}"
            out.append({"id": bid, "name": f"{cuisine} {i}", "review_count": i, "rating": 4.5,
                        "coordinates": {"latitude": 40.7, "longitude": -74.0},
                        "location": {"display_address": [f"{i} Main St", "New York, NY 10001"],
                                     "zip_code": "10001" if i % 7 else "11201"}})
        return out
    monkeypatch.setattr(yelp_scrape, "yelp_search", fake_search)
    monkeypatch.setattr(yelp_scrape.time, "sleep", lambda s: None)
    with mock_aws():
        make_table()
        n = yelp_scrape.scrape("k", 200, backup=str(tmp_path / "r.json"))
        items = boto3.resource("dynamodb").Table("yelp-restaurants").scan()["Items"]
        assert n == len(items) == 7 * 200
        assert len({i["BusinessID"] for i in items}) == len(items)
        it = items[0]
        for f in ("BusinessID", "Name", "Address", "Coordinates", "NumberOfReviews", "Rating", "ZipCode",
                  "insertedAtTimestamp", "Cuisine"):
            assert f in it, f
        assert all(i["ZipCode"].startswith("100") for i in items)


class FakeOS(BaseHTTPRequestHandler):
    docs, indices = {}, set()

    def _send(self, code, body):
        data = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *a):
        pass

    def _body(self):
        n = int(self.headers.get("Content-Length") or 0)
        return self.rfile.read(n).decode() if n else ""

    def do_GET(self):
        assert self.headers["Authorization"].startswith("Basic ")
        p = urlparse(self.path).path
        if p.endswith("/_count"):
            return self._send(200, {"count": len(self.docs)})
        return self._send(200 if p.strip("/") in self.indices else 404, {})

    def do_PUT(self):
        body = json.loads(self._body())
        assert body["mappings"]["properties"]["Cuisine"]["type"] == "keyword"
        self.indices.add(self.path.strip("/"))
        self._send(200, {"acknowledged": True})

    def do_DELETE(self):
        self.indices.discard(self.path.strip("/"))
        self._send(200, {})

    def do_POST(self):
        body = self._body()
        if self.path == "/_bulk":
            assert self.headers["Content-Type"] == "application/x-ndjson" and body.endswith("\n")
            lines = body.strip().split("\n")
            for a, d in zip(lines[::2], lines[1::2]):
                self.docs[json.loads(a)["index"]["_id"]] = json.loads(d)
            return self._send(200, {"errors": False})
        self._send(200, {})


def test_opensearch_load():
    srv = HTTPServer(("127.0.0.1", 0), FakeOS)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    with mock_aws():
        make_table()
        t = boto3.resource("dynamodb").Table("yelp-restaurants")
        for i in range(1203):
            t.put_item(Item={"BusinessID": f"b{i}", "Cuisine": "thai", "Name": "x"})
        n = opensearch_load.load(f"http://127.0.0.1:{srv.server_port}", "u", "p")
    assert n == 1203
    assert FakeOS.docs["b5"] == {"RestaurantID": "b5", "Cuisine": "thai", "type": "Restaurant"}
    srv.shutdown()
