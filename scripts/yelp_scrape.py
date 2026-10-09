"""Scrape ~200 Manhattan restaurants per cuisine from the Yelp Fusion API and store them
in the DynamoDB table "yelp-restaurants".

Usage:  YELP_API_KEY=... python3 scripts/yelp_scrape.py [--per-cuisine 200]
"""
import argparse
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from decimal import Decimal

import boto3

YELP_URL = "https://api.yelp.com/v3/businesses/search"
CUISINES = ["chinese", "japanese", "italian", "mexican", "indian", "thai", "korean"]
# Yelp caps offset+limit at 240 per query, so we search several Manhattan areas to reach ~200 unique each.
AREAS = [
    "Manhattan, New York, NY",
    "Midtown Manhattan, New York, NY",
    "Lower Manhattan, New York, NY",
    "Upper West Side, New York, NY",
    "Upper East Side, New York, NY",
    "East Village, New York, NY",
    "Chelsea, New York, NY",
    "Harlem, New York, NY",
]
MANHATTAN_ZIP_PREFIXES = ("100", "101", "102")


def yelp_search(api_key, term, location, offset, limit=50):
    qs = urllib.parse.urlencode({"term": term, "location": location, "categories": "restaurants",
                                 "limit": limit, "offset": offset})
    req = urllib.request.Request(f"{YELP_URL}?{qs}", headers={"Authorization": f"Bearer {api_key}"})
    for attempt in range(5):
        try:
            with urllib.request.urlopen(req, timeout=20) as resp:
                return json.loads(resp.read()).get("businesses", [])
        except urllib.error.HTTPError as e:
            body = e.read().decode(errors="ignore")
            if e.code == 429:
                time.sleep(2 ** attempt)
                continue
            if e.code == 400 and "LOCATION" in body.upper():
                return []
            raise RuntimeError(f"Yelp API error {e.code}: {body[:300]}") from None
    return []


def to_item(biz, cuisine):
    loc = biz.get("location") or {}
    coords = biz.get("coordinates") or {}
    address = ", ".join(loc.get("display_address") or []) or loc.get("address1") or ""
    item = {
        "BusinessID": biz["id"],
        "Name": biz.get("name", ""),
        "Address": address,
        "Coordinates": {
            "Latitude": Decimal(str(coords.get("latitude"))) if coords.get("latitude") is not None else None,
            "Longitude": Decimal(str(coords.get("longitude"))) if coords.get("longitude") is not None else None,
        },
        "NumberOfReviews": int(biz.get("review_count", 0)),
        "Rating": Decimal(str(biz.get("rating", 0))),
        "ZipCode": loc.get("zip_code", ""),
        "Cuisine": cuisine,
        "insertedAtTimestamp": datetime.now(timezone.utc).isoformat(),
    }
    if biz.get("price"):
        item["Price"] = biz["price"]
    if biz.get("display_phone"):
        item["Phone"] = biz["display_phone"]
    item["Coordinates"] = {k: v for k, v in item["Coordinates"].items() if v is not None}
    return item


def scrape(api_key, per_cuisine=200, table_name="yelp-restaurants", region=None, backup="restaurants.json"):
    table = boto3.resource("dynamodb", region_name=region).Table(table_name)
    seen, all_items = set(), []
    for cuisine in CUISINES:
        got = 0
        for area in AREAS:
            if got >= per_cuisine:
                break
            for offset in (0, 50, 100, 150, 190):
                if got >= per_cuisine:
                    break
                batch = yelp_search(api_key, f"{cuisine} restaurants", area, offset)
                if not batch:
                    break
                for biz in batch:
                    zip_code = (biz.get("location") or {}).get("zip_code") or ""
                    if biz["id"] in seen or not zip_code.startswith(MANHATTAN_ZIP_PREFIXES):
                        continue
                    seen.add(biz["id"])
                    all_items.append(to_item(biz, cuisine))
                    got += 1
                    if got >= per_cuisine:
                        break
                time.sleep(0.25)
        print(f"  {cuisine:<9} {got} restaurants")

    with table.batch_writer(overwrite_by_pkeys=["BusinessID"]) as writer:
        for item in all_items:
            writer.put_item(Item=item)
    with open(backup, "w") as f:
        json.dump(all_items, f, default=str, indent=1)
    print(f"Stored {len(all_items)} unique restaurants in DynamoDB table '{table_name}' (backup: {backup}).")
    return len(all_items)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--per-cuisine", type=int, default=200)
    ap.add_argument("--table", default="yelp-restaurants")
    args = ap.parse_args()
    key = os.environ.get("YELP_API_KEY") or input("Yelp API key: ").strip()
    scrape(key, args.per_cuisine, args.table)
