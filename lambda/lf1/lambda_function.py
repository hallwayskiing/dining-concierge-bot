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
    attrs = attrs if attrs is not None else dict(event["sessionState"].get("sessionAttributes") or {})
    attrs["eliciting"] = slot_name
    return {
        "sessionState": {
            "sessionAttributes": attrs,
            "dialogAction": {"type": "ElicitSlot", "slotToElicit": slot_name},
            "intent": {"name": intent["name"], "slots": slots, "state": "InProgress"},
        },
        "messages": [{"contentType": "PlainText", "content": message}],
    }


def close(event, message, slots=None, attrs=None):
    intent = event["sessionState"]["intent"]
    attrs = attrs if attrs is not None else dict(event["sessionState"].get("sessionAttributes") or {})
    attrs.pop("eliciting", None)
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
PERIODS = {"MO": "09:00", "AF": "14:00", "EV": "19:00", "NI": "21:00"}  # Lex values for "morning", "evening"...
TIME_RE = re.compile(r"\b(\d{1,2})(?:[:.](\d{2}))?(?::\d{2})?\s*(a\.?\s?m\.?|p\.?\s?m\.?)?(?![\d.])", re.I)
WORD_TIMES = {"noon": "12:00", "midday": "12:00", "midnight": "00:00", "tonight": "19:00",
              "evening": "19:00", "dinner": "19:00", "lunch": "12:00", "afternoon": "14:00"}


def parse_time(*candidates):
    """Normalize anything Lex (or the user) gives us to HH:MM, or None.

    Lex V2's AMAZON.Time can return "19:00", "19:00:00", ambiguous resolved values ["07:00", "19:00"],
    a period code like "EV", or only the raw text ("7 pm, please"). A bare hour is read as dinner time.
    """
    for c in candidates:
        if not c:
            continue
        c = str(c).strip()
        if c.upper() in PERIODS:
            return PERIODS[c.upper()]
        low = c.lower()
        for word, hhmm in WORD_TIMES.items():
            if re.search(r"\b" + word + r"\b", low) and not re.search(r"\d", low):
                return hhmm
        m = TIME_RE.search(c)
        if not m:
            continue
        hh, mm, mer = int(m.group(1)), int(m.group(2) or 0), (m.group(3) or "").lower().replace(".", "").replace(" ", "")
        if mm > 59 or hh > 23:
            continue
        if mer == "pm" and hh < 12:
            hh += 12
        elif mer == "am" and hh == 12:
            hh = 0
        elif not mer and 1 <= hh <= 10 and m.group(2) is None:
            hh += 12  # "7" -> 19:00
        return f"{hh:02d}:{mm:02d}"
    return None


def time_candidates(slots, transcript):
    v = ((slots or {}).get("DiningTime") or {}).get("value") or {}
    # For ambiguous input Lex gives resolvedValues like ["07:00", "19:00"]: try the later (evening) one first.
    return [v.get("interpretedValue"), *reversed(v.get("resolvedValues") or []), v.get("originalValue"), transcript]


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


def validate(slots, transcript=None, eliciting=None):
    """Return (slot_name, error_message) for the first invalid slot, normalizing values in place.

    `eliciting` is the slot we asked for last turn; if Lex couldn't fill it, we try the raw transcript.
    """
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
    if num is None and eliciting == "NumberOfPeople" and transcript:
        words = {w: str(i) for i, w in enumerate("zero one two three four five six seven eight nine ten eleven "
                                                 "twelve thirteen fourteen fifteen sixteen seventeen eighteen "
                                                 "nineteen twenty".split())}
        m = re.search(r"\d+", transcript) or re.search(r"\b(" + "|".join(words) + r")\b", transcript.lower())
        num = (m.group(0) if m.group(0).isdigit() else words[m.group(0)]) if m else "0"
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

    raw_t = slot_text(slots, "DiningTime")
    from_transcript = transcript if eliciting == "DiningTime" else None
    t = parse_time(*time_candidates(slots, from_transcript)) if (raw_t or from_transcript) else None
    if (raw_t or from_transcript) and t is None:
        return "DiningTime", "Sorry, I didn't catch the time. Please give me a time like 7 pm or 19:30."
    if t is not None:
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

    eliciting = attrs.pop("eliciting", None)
    if slot_text(slots, "Location") is None:  # fresh request: ignore what an abandoned one was asking
        eliciting = None
    bad_slot, error = validate(slots, event.get("inputTranscript"), eliciting)
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
    intent = event["sessionState"]["intent"]
    print(json.dumps({"sessionId": event.get("sessionId"), "inputTranscript": event.get("inputTranscript"),
                      "source": event.get("invocationSource"), "intent": intent.get("name"),
                      "slots": {k: (v or {}).get("value") for k, v in (intent.get("slots") or {}).items()}}))
    name = event["sessionState"]["intent"]["name"]
    if name == "GreetingIntent":
        return close(event, "Hi there, how can I help?")
    if name == "ThankYouIntent":
        return close(event, "You're welcome. Enjoy your meal!")
    if name == "DiningSuggestionsIntent":
        return handle_dining(event)
    return close(event, "Sorry, I can help you find restaurants. Try saying 'I need restaurant suggestions'.")
