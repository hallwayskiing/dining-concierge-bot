"""LF0 - Chat API handler.

API Gateway (POST /chatbot, Lambda proxy integration) -> this function -> Amazon Lex V2.
Request/response bodies follow the BotRequest / BotResponse models in swagger.yaml.
"""
import json
import os
import re
import uuid
from datetime import datetime, timezone

import boto3

lex = boto3.client("lexv2-runtime")

BOT_ID = os.environ["BOT_ID"]
BOT_ALIAS_ID = os.environ["BOT_ALIAS_ID"]
LOCALE_ID = os.environ.get("LOCALE_ID", "en_US")

CORS_HEADERS = {
    "Content-Type": "application/json",
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token",
    "Access-Control-Allow-Methods": "OPTIONS,POST",
}

FALLBACK_TEXT = "Sorry, I didn't catch that. Could you say it another way?"


def _now():
    return datetime.now(timezone.utc).isoformat()


def _respond(status, body):
    return {"statusCode": status, "headers": CORS_HEADERS, "body": json.dumps(body)}


def _bot_message(text, msg_id):
    return {
        "type": "unstructured",
        "unstructured": {"id": msg_id, "text": text, "timestamp": _now()},
    }


def _session_id(raw):
    """Lex V2 session ids must match [0-9a-zA-Z._:-]{2,100}."""
    sid = re.sub(r"[^0-9a-zA-Z._:-]", "", raw or "")[:100]
    return sid if len(sid) >= 2 else "anon-" + uuid.uuid4().hex


def lambda_handler(event, context):
    # 1. Extract the text message from the API request
    try:
        body = event.get("body") or "{}"
        if isinstance(body, str):
            body = json.loads(body)
        first = body["messages"][0]["unstructured"]
        text = (first.get("text") or "").strip()
    except (KeyError, IndexError, TypeError, json.JSONDecodeError):
        return _respond(400, {"code": 400, "message": "Body must follow the BotRequest schema."})
    if not text:
        return _respond(400, {"code": 400, "message": "Message text is empty."})

    # The frontend sends a persistent per-browser id; it doubles as the Lex session id
    # and as the key for remembering the user's last search (extra credit).
    session_id = _session_id(first.get("id"))

    # 2-3. Send it to the Lex chatbot and wait for the response
    try:
        resp = lex.recognize_text(
            botId=BOT_ID,
            botAliasId=BOT_ALIAS_ID,
            localeId=LOCALE_ID,
            sessionId=session_id,
            text=text,
        )
    except Exception as exc:  # surface a friendly error to the UI, details to CloudWatch
        print(f"Lex error: {exc!r}")
        return _respond(500, {"code": 500, "message": "The concierge is unavailable right now."})

    # 4. Send back Lex's response as the API response
    lex_messages = [m.get("content") for m in resp.get("messages", []) if m.get("content")]
    if not lex_messages:
        lex_messages = [FALLBACK_TEXT]
    return _respond(200, {"messages": [_bot_message(t, session_id) for t in lex_messages]})
