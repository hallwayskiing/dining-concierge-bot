"""LF2 - Suggestions queue worker (runs every minute via EventBridge).

For each request in SQS (Q1):
  1. get random restaurant IDs for the cuisine from OpenSearch (index "restaurants"),
  2. look up name/address/rating in DynamoDB ("yelp-restaurants"),
  3. format and email them to the user with SES,
  4. remember what was sent (extra credit) and delete the message.
"""
import base64
import json
import os
import urllib.request
from datetime import date, datetime, timezone

import boto3

QUEUE_URL = os.environ["QUEUE_URL"]
RESTAURANT_TABLE = os.environ.get("RESTAURANT_TABLE", "yelp-restaurants")
STATE_TABLE = os.environ.get("STATE_TABLE", "dining-user-state")
OS_ENDPOINT = os.environ.get("OS_ENDPOINT", "")          # e.g. https://search-xxx.us-east-1.es.amazonaws.com
OS_USER = os.environ.get("OS_USER", "")
OS_PASSWORD = os.environ.get("OS_PASSWORD", "")
OS_INDEX = os.environ.get("OS_INDEX", "restaurants")
SENDER = os.environ["SENDER_EMAIL"]
NUM_SUGGESTIONS = int(os.environ.get("NUM_SUGGESTIONS", "3"))

sqs = boto3.client("sqs")
ses = boto3.client("ses")
dynamodb = boto3.resource("dynamodb")
restaurants = dynamodb.Table(RESTAURANT_TABLE)
state_table = dynamodb.Table(STATE_TABLE)


# ---------------------------------------------------------------- OpenSearch
def os_request(method, path, body=None):
    req = urllib.request.Request(OS_ENDPOINT.rstrip("/") + path, method=method,
                                 data=json.dumps(body).encode() if body is not None else None)
    req.add_header("Content-Type", "application/json")
    token = base64.b64encode(f"{OS_USER}:{OS_PASSWORD}".encode()).decode()
    req.add_header("Authorization", f"Basic {token}")
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read())


def random_restaurant_ids(cuisine, k=NUM_SUGGESTIONS):
    query = {
        "size": k,
        "_source": ["RestaurantID", "Cuisine"],
        "query": {
            "function_score": {
                "query": {"term": {"Cuisine": cuisine.lower()}},
                "random_score": {},
                "boost_mode": "replace",
            }
        },
    }
    hits = os_request("POST", f"/{OS_INDEX}/_search", query)["hits"]["hits"]
    return [h["_source"]["RestaurantID"] for h in hits]


# ---------------------------------------------------------------- DynamoDB
def restaurant_details(ids):
    if not ids:
        return []
    resp = dynamodb.batch_get_item(RequestItems={RESTAURANT_TABLE: {"Keys": [{"BusinessID": i} for i in ids]}})
    by_id = {item["BusinessID"]: item for item in resp["Responses"].get(RESTAURANT_TABLE, [])}
    return [by_id[i] for i in ids if i in by_id]   # keep the random order


# ---------------------------------------------------------------- email
def pretty_time(hhmm):
    try:
        return datetime.strptime(hhmm, "%H:%M").strftime("%I:%M %p").lstrip("0").replace(":00 ", " ").lower()
    except (TypeError, ValueError):
        return hhmm or ""


def pretty_date(iso):
    try:
        d = date.fromisoformat(iso)
    except (TypeError, ValueError):
        return iso or ""
    try:
        from zoneinfo import ZoneInfo
        today = datetime.now(ZoneInfo("America/New_York")).date()
    except Exception:
        today = datetime.now(timezone.utc).date()
    return "today" if d == today else d.strftime("%A, %B %-d")


def compose(req, items):
    cuisine = req["cuisine"].title()
    people = req.get("numPeople") or ""
    party = f" for {people} {'person' if str(people) == '1' else 'people'}" if people else ""
    when = ""
    if req.get("date") or req.get("time"):
        when = f", for {pretty_date(req.get('date'))} at {pretty_time(req.get('time'))}"
    intro = ("Hello! As requested, here are the same " if req.get("reuse") else "Hello! Here are my ") + \
            f"{cuisine} restaurant suggestions{party}{when}:"
    lines = []
    for n, it in enumerate(items, 1):
        extra = []
        if it.get("Rating") is not None:
            extra.append(f"{it['Rating']}★")
        if it.get("NumberOfReviews") is not None:
            extra.append(f"{it['NumberOfReviews']} reviews")
        tail = f" ({', '.join(extra)})" if extra else ""
        lines.append(f"{n}. {it['Name']}, located at {it.get('Address', 'address unavailable')}{tail}")
    body = intro + "\n\n" + "\n".join(lines) + "\n\nEnjoy your meal!\n— Dining Concierge"
    subject = f"Your {cuisine} restaurant suggestions"
    return subject, body


def send_email(to, subject, body):
    ses.send_email(
        Source=SENDER,
        Destination={"ToAddresses": [to]},
        Message={"Subject": {"Data": subject}, "Body": {"Text": {"Data": body}}},
    )


# ---------------------------------------------------------------- worker
def process(req):
    ids = []
    if req.get("reuse"):
        last = state_table.get_item(Key={"userId": req["userId"]}).get("Item") or {}
        ids = list(last.get("lastRecommendations") or [])
    if not ids:
        req["reuse"] = False
        ids = random_restaurant_ids(req["cuisine"])
    items = restaurant_details(ids)
    if not items:
        subject = "Sorry — no restaurant suggestions found"
        body = f"Hello! I couldn't find any {req['cuisine'].title()} restaurants right now. Please try another cuisine."
    else:
        subject, body = compose(req, items)
    send_email(req["email"], subject, body)
    print(f"sent {len(items)} suggestions to {req['email']}")
    if items:
        state_table.update_item(
            Key={"userId": req["userId"]},
            UpdateExpression="SET lastRecommendations=:r, lastLocation=:l, lastCuisine=:c, lastEmail=:e",
            ExpressionAttributeValues={":r": [i["BusinessID"] for i in items], ":l": req["location"],
                                       ":c": req["cuisine"], ":e": req["email"]},
        )


def lambda_handler(event, context):
    processed = failed = 0
    for _ in range(5):  # up to 50 messages per run
        resp = sqs.receive_message(QueueUrl=QUEUE_URL, MaxNumberOfMessages=10, WaitTimeSeconds=1)
        messages = resp.get("Messages", [])
        if not messages:
            break
        for msg in messages:
            try:
                process(json.loads(msg["Body"]))
                sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=msg["ReceiptHandle"])
                processed += 1
            except Exception as exc:  # leave it on the queue to retry next minute
                failed += 1
                print(f"failed to process {msg.get('MessageId')}: {exc!r}")
    print(f"processed={processed} failed={failed}")
    return {"processed": processed, "failed": failed}
