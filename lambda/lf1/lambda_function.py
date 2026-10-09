"""LF1 - Amazon Lex V2 code hook for the Dining Concierge bot.

Handles GreetingIntent, ThankYouIntent and DiningSuggestionsIntent. For dining
suggestions it collects location, cuisine, party size, date, time and email,
validates each answer, pushes the request to SQS (Q1) and confirms to the user.

Extra credit: remembers each user's last search (DynamoDB). If they ask again for
the same location + cuisine, the bot offers to resend last time's recommendations.
"""
import json
import os
import re
from datetime import date, datetime, timedelta, timezone

import boto3

QUEUE_URL = os.environ["QUEUE_URL"]
STATE_TABLE = os.environ.get("STATE_TABLE", "dining-user-state")

sqs = boto3.client("sqs")
state_table = boto3.resource("dynamodb").Table(STATE_TABLE)

# Cuisines we scraped from Yelp. Keys are what we store/search; values are words users may type.
CUISINES = {
    "chinese": ["chinese", "dim sum", "szechuan", "sichuan", "cantonese"],
    "japanese": ["japanese", "sushi", "ramen", "izakaya"],
    "italian": ["italian", "pizza", "pasta"],
    "mexican": ["mexican", "taco", "tacos", "burrito"],
    "indian": ["indian", "curry"],
    "thai": ["thai"],
    "korean": ["korean", "kbbq", "bibimbap"],
}
LOCATION_WORDS = ["manhattan", "new york", "nyc", "ny city", "midtown", "soho", "harlem",
                  "chelsea", "tribeca", "east village", "west village", "upper east side",
                  "upper west side", "lower east side", "financial district", "greenwich village"]
EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")

SLOT_ORDER = ["Location", "Cuisine", "NumberOfPeople", "DiningDate", "DiningTime", "Email"]
PROMPTS = {
    "Location": "Great. I can help you with that. What city or city area are you looking to dine in?",
    "Cuisine": "What cuisine would you like to try? I know Chinese, Japanese, Italian, Mexican, Indian, Thai and Korean.",
    "NumberOfPeople": "Ok, how many people are in your party?",
    "DiningDate": "A few more to go. What date?",
    "DiningTime": "What time?",
    "Email": "Great. Lastly, I need your email address so I can send you my findings.",
}

try:
    from zoneinfo import ZoneInfo
    NY_TZ = ZoneInfo("America/New_York")
except Exception:  # pragma: no cover - tzdata missing
    NY_TZ = timezone(timedelta(hours=-4))


# ---------------------------------------------------------------- Lex helpers
def slot_text(slots, name):
    """Best value Lex gave for a slot (interpreted, else first resolved, else original)."""
    slot = (slots or {}).get(name)
    if not slot or not slot.get("value"):
        return None
    v = slot["value"]
    if v.get("interpretedValue"):
        return v["interpretedValue"]
    resolved = v.get("resolvedValues") or []
    if resolved:
        # Ambiguous times like "7" resolve to ["07:00", "19:00"]; prefer the evening one.
        return resolved[-1]
    return v.get("originalValue")


def set_slot(slots, name, value):
    slots[name] = None if value is None else {
        "value": {"originalValue": value, "interpretedValue": value, "resolvedValues": [value]}
    }


def elicit(event, slots, slot_name, message, attrs=None):
    intent = event["sessionState"]["intent"]
    return {
        "sessionState": {
            "sessionAttributes": attrs if attrs is not None else event["sessionState"].get("sessionAttributes", {}),
            "dialogAction": {"type": "ElicitSlot", "slotToElicit": slot_name},
            "intent": {"name": intent["name"], "slots": slots, "state": "InProgress"},
        },
        "messages": [{"contentType": "PlainText", "content": message}],
    }


def close(event, message, slots=None, attrs=None):
    intent = event["sessionState"]["intent"]
    return {
        "sessionState": {
            "sessionAttributes": attrs if attrs is not None else event["sessionState"].get("sessionAttributes", {}),
            "dialogAction": {"type": "Close"},
            "intent": {"name": intent["name"], "slots": slots if slots is not None else intent.get("slots"),
                       "state": "Fulfilled"},
        },
        "messages": [{"contentType": "PlainText", "content": message}],
    }


# ---------------------------------------------------------------- validation
def normalize_location(text):
    t = text.lower()
    return "Manhattan" if any(w in t for w in LOCATION_WORDS) else None


def normalize_cuisine(text):
    t = text.lower()
    for key, words in CUISINES.items():
        if any(re.search(r"\b" + re.escape(w) + r"\b", t) for w in words):
            return key
    return None


def now_ny():
    return datetime.now(NY_TZ)


def validate(slots):
    """Return (slot_name, error_message) for the first invalid slot, normalizing values in place."""
    loc = slot_text(slots, "Location")
    if loc is not None:
        norm = normalize_location(loc)
        if not norm:
            return "Location", f"Sorry, I can't fulfill requests for {loc.strip().rstrip('.')}. Please enter a valid location (I currently cover Manhattan)."
        set_slot(slots, "Location", norm)

    cui = slot_text(slots, "Cuisine")
    if cui is not None:
        norm = normalize_cuisine(cui)
        if not norm:
            return "Cuisine", ("Sorry, I don't have suggestions for that cuisine yet. "
                               "Try Chinese, Japanese, Italian, Mexican, Indian, Thai or Korean.")
        set_slot(slots, "Cuisine", norm)

    num = slot_text(slots, "NumberOfPeople")
    if num is not None:
        try:
            n = int(float(num))
        except ValueError:
            n = 0
        if not 1 <= n <= 20:
            return "NumberOfPeople", "I can book for parties of 1 to 20. How many people are in your party?"
        set_slot(slots, "NumberOfPeople", str(n))

    d = slot_text(slots, "DiningDate")
    dining_date = None
    if d is not None:
        try:
            dining_date = date.fromisoformat(d)
        except ValueError:
            return "DiningDate", "I didn't understand that date. What date would you like to dine?"
        if dining_date < now_ny().date():
            return "DiningDate", "That date has already passed. What date would you like to dine?"

    t = slot_text(slots, "DiningTime")
    if t is not None:
        if not re.fullmatch(r"\d{2}:\d{2}", t):
            return "DiningTime", "Please give me a specific time, like 7 pm."
        if dining_date and dining_date == now_ny().date():
            hh, mm = map(int, t.split(":"))
            if (hh, mm) <= (now_ny().hour, now_ny().minute):
                return "DiningTime", "That time has already passed today. What time would you like?"
        set_slot(slots, "DiningTime", t)

    email = slot_text(slots, "Email")
    if email is not None:
        m = EMAIL_RE.search(email.replace(" at ", "@").replace(" dot ", "."))
        if not m:
            return "Email", "That doesn't look like a valid email address. Could you type it again?"
        set_slot(slots, "Email", m.group(0).lower())
    return None, None


ACKS = {
    "Cuisine": lambda s: f"Got it, {slot_text(s, 'Location')}. ",
    "NumberOfPeople": lambda s: f"{slot_text(s, 'Cuisine').title()}, nice choice. ",
}


# ---------------------------------------------------------------- state (extra credit)
def get_last_search(user_id):
    try:
        return state_table.get_item(Key={"userId": user_id}).get("Item")
    except Exception as exc:
        print(f"state lookup failed: {exc!r}")
        return None


def save_search(user_id, req):
    try:
        state_table.update_item(
            Key={"userId": user_id},
            UpdateExpression=("SET lastLocation=:l, lastCuisine=:c, lastEmail=:e, lastNumPeople=:n, "
                              "lastDate=:d, lastTime=:t, updatedAt=:u"),
            ExpressionAttributeValues={
                ":l": req["location"], ":c": req["cuisine"], ":e": req["email"], ":n": req["numPeople"],
                ":d": req["date"], ":t": req["time"], ":u": datetime.now(timezone.utc).isoformat(),
            },
        )
    except Exception as exc:
        print(f"state save failed: {exc!r}")


def push(req):
    sqs.send_message(QueueUrl=QUEUE_URL, MessageBody=json.dumps(req))
    print(f"queued request: {req}")


# ---------------------------------------------------------------- intents
def handle_dining(event):
    slots = event["sessionState"]["intent"].get("slots") or {}
    attrs = event["sessionState"].get("sessionAttributes") or {}
    user_id = event.get("sessionId", "anonymous")
    if slot_text(slots, "Location") is None:  # a fresh request: forget flags from an earlier one
        for k in ("sameAsLastResolved", "sameAsLastTries"):
            attrs.pop(k, None)

    bad_slot, error = validate(slots)
    if bad_slot:
        set_slot(slots, bad_slot, None)
        return elicit(event, slots, bad_slot, error, attrs)

    location, cuisine = slot_text(slots, "Location"), slot_text(slots, "Cuisine")

    # Extra credit: same location + cuisine as last time -> offer the same recommendations.
    if location and cuisine and attrs.get("sameAsLastResolved") != "true":
        last = get_last_search(user_id)
        if last and last.get("lastLocation") == location and last.get("lastCuisine") == cuisine:
            answer = slot_text(slots, "SameAsLast")
            if answer is None:
                tries = int(attrs.get("sameAsLastTries", "0"))
                if tries < 2:
                    attrs["sameAsLastTries"] = str(tries + 1)
                    return elicit(event, slots, "SameAsLast",
                                  f"Welcome back! Last time you also asked for {cuisine.title()} food in {location}. "
                                  "Would you like me to send you the same recommendations as last time? (yes/no)",
                                  attrs)
                answer = "No"
            attrs["sameAsLastResolved"] = "true"
            if answer.lower().startswith("y"):
                req = {"userId": user_id, "reuse": True, "location": location, "cuisine": cuisine,
                       "email": last["lastEmail"], "numPeople": last.get("lastNumPeople", ""),
                       "date": last.get("lastDate", ""), "time": last.get("lastTime", "")}
                push(req)
                for k in ("sameAsLastResolved", "sameAsLastTries"):
                    attrs.pop(k, None)
                return close(event, f"You got it. I'll resend last time's {cuisine.title()} suggestions "
                                    f"to {last['lastEmail']} shortly. Enjoy!", slots, attrs)

    for name in SLOT_ORDER:
        if slot_text(slots, name) is None:
            prefix = ACKS[name](slots) if name in ACKS else ""
            return elicit(event, slots, name, prefix + PROMPTS[name], attrs)

    # All slots collected and valid -> push to Q1 and confirm.
    req = {
        "userId": user_id,
        "reuse": False,
        "location": location,
        "cuisine": cuisine,
        "numPeople": slot_text(slots, "NumberOfPeople"),
        "date": slot_text(slots, "DiningDate"),
        "time": slot_text(slots, "DiningTime"),
        "email": slot_text(slots, "Email"),
    }
    push(req)
    save_search(user_id, req)
    for k in ("sameAsLastResolved", "sameAsLastTries"):
        attrs.pop(k, None)
    return close(event, "You're all set. Expect my suggestions in your inbox shortly! Have a good day.",
                 slots, attrs)


def lambda_handler(event, context):
    print(json.dumps({k: event.get(k) for k in ("sessionId", "inputTranscript", "invocationSource")}))
    name = event["sessionState"]["intent"]["name"]
    if name == "GreetingIntent":
        return close(event, "Hi there, how can I help?")
    if name == "ThankYouIntent":
        return close(event, "You're welcome. Enjoy your meal!")
    if name == "DiningSuggestionsIntent":
        return handle_dining(event)
    return close(event, "Sorry, I can help you find restaurants. Try saying 'I need restaurant suggestions'.")
