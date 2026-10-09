# Dining Concierge Chatbot — Cloud Computing & Big Data, Fall 2026, HW1

Ethan Nie (yn2465@nyu.edu)

A serverless, microservice-driven chatbot that collects dining preferences in conversation and emails
restaurant suggestions.

```
S3 (frontend) → API Gateway (POST /v1/chatbot) → LF0 → Lex V2 (DiningConciergeBot) → LF1 → SQS (Q1)
                                                                                         ↓
                    SES email ← LF2 (EventBridge, every minute) → OpenSearch "restaurants" + DynamoDB "yelp-restaurants"
```

## Repository layout

| Path | What it is |
|---|---|
| `frontend/` | Starter frontend, wired to the API (persistent per-browser user id, HTML-escaped messages) |
| `api/swagger.yaml` | API spec from the starter repo (imported into API Gateway; `swagger.json` is the same spec) |
| `lambda/lf0/` | **LF0** – API handler: extracts the message, calls Lex V2 `RecognizeText`, returns `BotResponse` |
| `lambda/lf1/` | **LF1** – Lex code hook: `GreetingIntent`, `ThankYouIntent`, `DiningSuggestionsIntent`; validates slots, pushes to Q1 |
| `lambda/lf2/` | **LF2** – queue worker: Q1 → random restaurants by cuisine from OpenSearch → details from DynamoDB → SES |
| `scripts/yelp_scrape.py` | Yelp Fusion scraper: 7 cuisines × ~200 = 1,400 unique Manhattan restaurants → DynamoDB |
| `scripts/opensearch_load.py` | Creates index `restaurants` and loads `RestaurantID` + `Cuisine` (type `Restaurant`) |
| `deploy.py` | Idempotent boto3 script that builds/tears down every AWS resource |
| `tests/` | Offline tests (moto) for all Lambdas, the scraper, the loader, and a dry run of `deploy.py` |

## Deploy (AWS CloudShell, us-east-1)

```bash
unzip dining-concierge-hw1.zip && cd hw1
python3 deploy.py core --email you@example.com     # IAM, DynamoDB, SQS, Lambdas, Lex, API, S3, SES (~5 min)
#   -> click the SES verification link that arrives in your inbox
python3 deploy.py scrape --yelp-key YOUR_YELP_KEY  # 1,000+ restaurants into DynamoDB
python3 deploy.py opensearch                       # OpenSearch domain + index + data, enables LF2 schedule (~20 min)
python3 deploy.py test --run-worker                # scripted conversation through the live API, then emails you
python3 deploy.py status                           # URLs and ids
python3 deploy.py delete-opensearch                # IMPORTANT: stop OpenSearch billing after the demo
```

## Requirement mapping

1. **Frontend** – `frontend/` hosted as an S3 static website (`chat.html`); the deploy script injects the API URL into `apigClient.js`.
2. **API** – API Gateway REST API imported from `swagger.yaml`, `POST /chatbot` → LF0 (Lambda proxy), `OPTIONS` mock + gateway responses for CORS, stage `v1`.
3. **Lex bot** – Lex V2 bot `DiningConciergeBot` (en_US) with the three intents; LF1 is the dialog + fulfillment code hook. Slots: Location, Cuisine, NumberOfPeople, DiningDate, DiningTime, Email. Invalid locations (only Manhattan is served), unknown cuisines, party sizes outside 1–20, past dates/times and malformed emails are re-asked. Requests go to SQS `Q1` (dead-letter queue after 5 failed attempts) and the user is told to expect an email.
4. **Lex ↔ API** – LF0 calls `lexv2-runtime.recognize_text` and returns Lex's messages in the `BotResponse` format.
5. **Yelp → DynamoDB** – table `yelp-restaurants` (key `BusinessID`) with Name, Address, Coordinates, NumberOfReviews, Rating, ZipCode, Cuisine, `insertedAtTimestamp`. Duplicates are skipped; only Manhattan ZIP codes (100xx–102xx) are kept. Several Manhattan neighborhoods are searched because Yelp caps each query at 240 results.
6. **OpenSearch** – domain `restaurants-domain` (1 × t3.small.search, 1 AZ, no standby, fine-grained access control with a master user). Index `restaurants`; each document stores `RestaurantID`, `Cuisine` and `type: "Restaurant"` (OpenSearch no longer supports mapping types, so the type is kept as a field).
7. **Suggestions module** – LF2 polls Q1, runs a `function_score`/`random_score` query filtered by cuisine, batch-gets details from DynamoDB, emails via SES, deletes the message. EventBridge rule `dining-lf2-every-minute` invokes it.

**Extra credit** – the frontend sends a persistent user id that LF0 uses as the Lex session id. LF1 saves each user's last search in DynamoDB table `dining-user-state`, and LF2 stores the restaurants it sent. When the same user asks again for the same location and cuisine, the bot asks whether they want the same recommendations as last time; "yes" re-sends them, "no" continues the normal conversation.

## Example conversation

```
User: Hello
Bot:  Hi there, how can I help?
User: I need some restaurant suggestions.
Bot:  Great. I can help you with that. What city or city area are you looking to dine in?
User: New Delhi
Bot:  Sorry, I can't fulfill requests for New Delhi. Please enter a valid location (I currently cover Manhattan).
User: Can you try Manhattan
Bot:  Got it, Manhattan. What cuisine would you like to try? ...
User: Japanese
Bot:  Japanese, nice choice. Ok, how many people are in your party?
User: Two
Bot:  A few more to go. What date?
User: Today
Bot:  What time?
User: 7 pm, please
Bot:  Great. Lastly, I need your email address so I can send you my findings.
User: you@example.com
Bot:  You're all set. Expect my suggestions in your inbox shortly! Have a good day.
```

## Notes

- SES starts in sandbox mode, so the recipient address must also be verified (the deploy script verifies the address you pass with `--email`).
- `python3 -m pytest -q tests` runs the offline test suite; `python3 tests/dryrun_deploy.py` dry-runs the deployment against moto with every Lex/OpenSearch call validated against the AWS API model.
- `python3 deploy.py destroy` removes everything.
