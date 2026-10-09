"""Offline tests for LF0/LF1/LF2 using moto. Run: python3 -m pytest -q tests"""
import importlib.util
import json
import os
import sys
from datetime import datetime, timedelta

import boto3
import pytest
from moto import mock_aws

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.environ.update(AWS_DEFAULT_REGION="us-east-1", AWS_ACCESS_KEY_ID="x", AWS_SECRET_ACCESS_KEY="x")


def load(name):
    spec = importlib.util.spec_from_file_location(name, os.path.join(ROOT, "lambda", name, "lambda_function.py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture
def aws():
    with mock_aws():
        sqs = boto3.client("sqs")
        q = sqs.create_queue(QueueName="Q1")["QueueUrl"]
        ddb = boto3.client("dynamodb")
        for t, k in (("yelp-restaurants", "BusinessID"), ("dining-user-state", "userId")):
            ddb.create_table(TableName=t, BillingMode="PAY_PER_REQUEST",
                             AttributeDefinitions=[{"AttributeName": k, "AttributeType": "S"}],
                             KeySchema=[{"AttributeName": k, "KeyType": "HASH"}])
        boto3.client("ses").verify_email_identity(EmailAddress="me@nyu.edu")
        os.environ.update(QUEUE_URL=q, SENDER_EMAIL="me@nyu.edu", BOT_ID="B", BOT_ALIAS_ID="A")
        yield {"queue": q, "sqs": sqs}


# ---------------------------------------------------------------- LF1 conversation simulator
class Lex:
    """Tiny stand-in for Lex V2: keeps slots/attrs, fills the elicited slot like Lex would."""
    BUILTIN = {"NumberOfPeople": {"Two": "2", "2": "2", "25": "25"},
               "DiningDate": {}, "DiningTime": {"7 pm, please": "19:00", "7": None, "3 am": "03:00"},
               "SameAsLast": {"yes": "Yes", "no": "No"}}

    def __init__(self, lf1, session="user-1"):
        self.lf1, self.session = lf1, session
        self.intent, self.slots, self.attrs, self.elicit = None, {}, {}, None

    def value(self, slot, text):
        if slot == "DiningDate":
            today = datetime.now(self.lf1.NY_TZ).date()
            v = {"Today": today, "Tomorrow": today + timedelta(1), "Yesterday": today - timedelta(1)}[text].isoformat()
            return {"originalValue": text, "interpretedValue": v, "resolvedValues": [v]}
        if slot == "DiningTime" and text == "7":
            return {"originalValue": text, "resolvedValues": ["07:00", "19:00"]}
        mapped = self.BUILTIN.get(slot, {}).get(text, text)
        return {"originalValue": text, "interpretedValue": mapped, "resolvedValues": [mapped]}

    def say(self, text, intent=None):
        if intent:
            self.intent, self.slots, self.elicit = intent, {}, None
        elif self.elicit:
            self.slots[self.elicit] = {"value": self.value(self.elicit, text)}
        event = {"sessionId": self.session, "inputTranscript": text, "invocationSource": "DialogCodeHook",
                 "sessionState": {"sessionAttributes": self.attrs,
                                  "intent": {"name": self.intent, "slots": self.slots, "state": "InProgress"}}}
        out = self.lf1.lambda_handler(json.loads(json.dumps(event)), None)
        ss = out["sessionState"]
        self.attrs = ss.get("sessionAttributes") or {}
        self.slots = ss["intent"].get("slots") or {}
        da = ss["dialogAction"]
        self.elicit = da.get("slotToElicit") if da["type"] == "ElicitSlot" else None
        if da["type"] == "Close":
            self.intent = None
        return out["messages"][0]["content"], da["type"]


def full_dining(bot, email="me@nyu.edu"):
    replies = [bot.say("I need some restaurant suggestions.", "DiningSuggestionsIntent")]
    for line in ["New Delhi", "Can you try Manhattan", "Japanese", "Two", "Today" if False else "Tomorrow",
                 "7 pm, please", email]:
        replies.append(bot.say(line))
    return replies


def test_greeting_and_thanks(aws):
    lf1 = load("lf1")
    bot = Lex(lf1)
    assert bot.say("Hello", "GreetingIntent") == ("Hi there, how can I help?", "Close")
    assert bot.say("Thank you!", "ThankYouIntent")[0].startswith("You're welcome")


def test_dining_flow_pushes_to_sqs(aws):
    lf1 = load("lf1")
    bot = Lex(lf1)
    r = full_dining(bot)
    texts = [t for t, _ in r]
    assert "What city" in texts[0]
    assert texts[1].startswith("Sorry, I can't fulfill requests for New Delhi")
    assert texts[2].startswith("Got it, Manhattan. What cuisine")
    assert texts[3].startswith("Japanese, nice choice. Ok, how many people")
    assert texts[4] == "A few more to go. What date?"
    assert texts[5] == "What time?"
    assert "email" in texts[6]
    assert r[-1] == ("You're all set. Expect my suggestions in your inbox shortly! Have a good day.", "Close")
    m = aws["sqs"].receive_message(QueueUrl=aws["queue"])["Messages"][0]
    body = json.loads(m["Body"])
    assert body["cuisine"] == "japanese" and body["location"] == "Manhattan" and body["numPeople"] == "2"
    assert body["time"] == "19:00" and body["email"] == "me@nyu.edu" and body["reuse"] is False


def test_validation(aws):
    lf1 = load("lf1")
    bot = Lex(lf1)
    bot.say("find me a restaurant", "DiningSuggestionsIntent")
    bot.say("Manhattan")
    assert bot.say("Martian food")[0].startswith("Sorry, I don't have suggestions")
    bot.say("some sushi please")
    assert bot.slots["Cuisine"]["value"]["interpretedValue"] == "japanese"
    assert bot.say("25")[0].startswith("I can book for parties of 1 to 20")
    bot.say("2")
    assert bot.say("Yesterday")[0].startswith("That date has already passed")
    bot.say("Tomorrow")
    bot.say("7")  # ambiguous -> evening
    assert bot.slots["DiningTime"]["value"]["interpretedValue"] == "19:00"
    assert bot.say("not an email")[0].startswith("That doesn't look like a valid email")
    assert bot.say("my email is Me@NYU.edu thanks")[1] == "Close"


def test_extra_credit_same_as_last(aws):
    lf1 = load("lf1")
    bot = Lex(lf1, session="returning-user")
    full_dining(bot)
    aws["sqs"].purge_queue(QueueUrl=aws["queue"])
    # come back later, same location + cuisine
    bot.say("I need restaurant suggestions", "DiningSuggestionsIntent")
    bot.say("Manhattan")
    text, kind = bot.say("japanese")
    assert "same recommendations as last time" in text and bot.elicit == "SameAsLast"
    text, kind = bot.say("yes")
    assert kind == "Close" and "resend" in text
    body = json.loads(aws["sqs"].receive_message(QueueUrl=aws["queue"])["Messages"][0]["Body"])
    assert body["reuse"] is True and body["email"] == "me@nyu.edu"
    # different cuisine -> no question
    bot.say("I need restaurant suggestions", "DiningSuggestionsIntent")
    bot.say("Manhattan")
    assert bot.say("italian")[0].startswith("Italian, nice choice")
    # same again, but answer no -> normal flow continues
    bot.say("I need restaurant suggestions", "DiningSuggestionsIntent")
    bot.say("Manhattan")
    bot.say("japanese")
    assert bot.say("no")[0].startswith("Japanese, nice choice. Ok, how many")


# ---------------------------------------------------------------- LF0
def test_lf0(aws, monkeypatch):
    lf0 = load("lf0")
    calls = {}

    def fake_recognize(**kw):
        calls.update(kw)
        return {"messages": [{"contentType": "PlainText", "content": "Hi there, how can I help?"}]}
    monkeypatch.setattr(lf0.lex, "recognize_text", fake_recognize)
    body = {"messages": [{"type": "unstructured", "unstructured": {"id": "user-abc 123!", "text": "Hello"}}]}
    out = lf0.lambda_handler({"body": json.dumps(body)}, None)
    assert out["statusCode"] == 200 and out["headers"]["Access-Control-Allow-Origin"] == "*"
    msgs = json.loads(out["body"])["messages"]
    assert msgs[0]["type"] == "unstructured" and msgs[0]["unstructured"]["text"] == "Hi there, how can I help?"
    assert calls["text"] == "Hello" and calls["sessionId"] == "user-abc123"
    assert lf0.lambda_handler({"body": "{}"}, None)["statusCode"] == 400


# ---------------------------------------------------------------- LF2
def test_lf2_sends_email_and_remembers(aws, monkeypatch):
    table = boto3.resource("dynamodb").Table("yelp-restaurants")
    for i in range(5):
        table.put_item(Item={"BusinessID": f"b{i}", "Name": f"Sushi {i}", "Address": f"{i} Commerce St, New York, NY 10014",
                             "Rating": 4, "NumberOfReviews": 100 + i, "Cuisine": "japanese"})
    lf2 = load("lf2")
    queries = []

    def fake_os(method, path, body=None):
        queries.append((method, path, body))
        return {"hits": {"hits": [{"_source": {"RestaurantID": f"b{i}"}} for i in (3, 1, 4)]}}
    monkeypatch.setattr(lf2, "os_request", fake_os)
    sent = []
    monkeypatch.setattr(lf2, "send_email", lambda to, s, b: sent.append((to, s, b)))

    req = {"userId": "u1", "reuse": False, "location": "Manhattan", "cuisine": "japanese", "numPeople": "2",
           "date": datetime.now().date().isoformat(), "time": "19:00", "email": "me@nyu.edu"}
    aws["sqs"].send_message(QueueUrl=aws["queue"], MessageBody=json.dumps(req))
    assert lf2.lambda_handler({}, None) == {"processed": 1, "failed": 0}
    to, subject, body = sent[0]
    print(body)
    assert to == "me@nyu.edu" and "Japanese restaurant suggestions for 2 people" in body
    assert body.index("Sushi 3") < body.index("Sushi 1") < body.index("Sushi 4")
    assert queries[0][2]["query"]["function_score"]["query"] == {"term": {"Cuisine": "japanese"}}
    state = boto3.resource("dynamodb").Table("dining-user-state").get_item(Key={"userId": "u1"})["Item"]
    assert state["lastRecommendations"] == ["b3", "b1", "b4"]

    # reuse request -> same restaurants, no OpenSearch call
    queries.clear()
    aws["sqs"].send_message(QueueUrl=aws["queue"], MessageBody=json.dumps({**req, "reuse": True}))
    lf2.lambda_handler({}, None)
    assert not queries and "same Japanese" in sent[1][2] and sent[1][2].index("Sushi 3") < sent[1][2].index("Sushi 1")
    assert "Messages" not in aws["sqs"].receive_message(QueueUrl=aws["queue"])


def test_lf2_real_ses_call(aws, monkeypatch):
    lf2 = load("lf2")
    lf2.send_email("me@nyu.edu", "subj", "body")  # moto validates the SES call shape


@pytest.mark.parametrize("candidates, expected", [
    (["19:00"], "19:00"), (["19:00:00"], "19:00"), (["EV"], "19:00"), ([None, "19:00", "07:00"], "19:00"),
    ([None, None, "7 pm, please"], "19:00"), ([None, None, None, "7pm please"], "19:00"),
    (["7:30 p.m."], "19:30"), (["12 am"], "00:00"), (["noon"], "12:00"), (["7"], "19:00"),
    (["11:45"], "11:45"), (["at 8:15 PM"], "20:15"), (["tomorrow"], None), (["yn2465@nyu.edu"], None),
])
def test_parse_time(aws, candidates, expected):
    assert load("lf1").parse_time(*candidates) == expected


def test_time_lex_returns_odd_values(aws):
    """Regression for the live run: Lex gave a non-HH:MM time for '7 pm, please'."""
    lf1 = load("lf1")
    for value in ({"originalValue": "7 pm, please", "interpretedValue": "19:00:00", "resolvedValues": ["19:00:00"]},
                  {"originalValue": "7 pm, please", "resolvedValues": []},
                  {"originalValue": "7 pm, please", "interpretedValue": "EV", "resolvedValues": ["EV"]}):
        bot = Lex(lf1)
        bot.say("I need restaurant suggestions", "DiningSuggestionsIntent")
        for line in ["Manhattan", "Japanese", "Two", "Tomorrow"]:
            bot.say(line)
        bot.value = lambda slot, text, v=value: v
        text, _ = bot.say("7 pm, please")
        assert text.startswith("Great. Lastly, I need your email"), (value, text)
        assert bot.slots["DiningTime"]["value"]["interpretedValue"] in ("19:00",)


def test_time_slot_not_filled_uses_transcript(aws):
    """If Lex can't fill AMAZON.Time at all, the dialog hook still gets the raw transcript."""
    lf1 = load("lf1")
    bot = Lex(lf1)
    bot.say("I need restaurant suggestions", "DiningSuggestionsIntent")
    for line in ["Manhattan", "Japanese", "Two", "Tomorrow"]:
        bot.say(line)
    event = {"sessionId": bot.session, "inputTranscript": "around 7:30pm please", "invocationSource": "DialogCodeHook",
             "sessionState": {"sessionAttributes": bot.attrs,
                              "intent": {"name": bot.intent, "slots": bot.slots, "state": "InProgress"}}}
    out = lf1.lambda_handler(event, None)
    assert out["sessionState"]["intent"]["slots"]["DiningTime"]["value"]["interpretedValue"] == "19:30"
    assert out["sessionState"]["dialogAction"]["slotToElicit"] == "Email"
