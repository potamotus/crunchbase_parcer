#!/usr/bin/env python3
"""
Server-friendly Crunchbase scraper using direct HTTP requests.
No browser needed — uses cookies extracted from browser_state.json.

Usage:
    # First: login locally and copy browser_state.json to server
    python3 run_server.py
"""

import csv
import json
import time
import random
import re
import sys
import urllib.parse
import urllib.request
from pathlib import Path

PROJECT_DIR = Path(__file__).parent
SOURCE_CSV = PROJECT_DIR / "data" / "source.csv"
OUTPUT_DIR = PROJECT_DIR / "output"
RESULTS_FILE = OUTPUT_DIR / "results.json"
STATE_FILE = OUTPUT_DIR / "browser_state.json"

ROW_START = 532
ROW_END = 1059
BATCH_SIZE = 25
BASE_URL = "https://www.crunchbase.com"


def load_cookies_from_state():
    """Extract cookies from Playwright browser_state.json."""
    with open(STATE_FILE) as f:
        state = json.load(f)
    cookie_str = "; ".join(
        f"{c['name']}={c['value']}"
        for c in state.get("cookies", [])
        if "crunchbase.com" in c.get("domain", "")
    )
    return cookie_str


def api_get(path, cookies):
    """GET request to Crunchbase API."""
    url = f"{BASE_URL}{path}"
    req = urllib.request.Request(url, headers={
        "Cookie": cookies,
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Accept": "application/json",
    })
    resp = urllib.request.urlopen(req, timeout=30)
    return json.loads(resp.read())


def api_post(path, body, cookies):
    """POST request to Crunchbase API."""
    url = f"{BASE_URL}{path}"
    data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST", headers={
        "Cookie": cookies,
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Accept": "application/json",
        "Content-Type": "application/json",
    })
    resp = urllib.request.urlopen(req, timeout=30)
    return json.loads(resp.read())


def normalize_city(city):
    city = re.sub(r"[Ōō]", "O", city)
    city = re.sub(r"[āǎ]", "a", city)
    city = re.sub(r"[ūǔ]", "u", city)
    city = re.sub(r"[éèê]", "e", city)
    city = re.sub(r"[ãâà]", "a", city)
    city = re.sub(r"[íì]", "i", city)
    city = re.sub(r"[óòô]", "o", city)
    city = re.sub(r"[úù]", "u", city)
    city = re.sub(r"[ñ]", "n", city)
    city = re.sub(r"[ç]", "c", city)
    city = re.sub(r"\s*\(.*?\)\s*", "", city)
    return city


COUNTRY_MAP = {"USA":["united states"],"United Kingdom":["united kingdom"],"Germany":["germany"],"Japan":["japan"],"China":["china"],"India":["india"],"France":["france"],"Canada":["canada"],"Brazil":["brazil"],"Italy":["italy"],"Netherlands":["netherlands"],"Russia":["russia"],"Norway":["norway"],"Portugal":["portugal"],"Saudi Arabia":["saudi arabia"],"Czech Republic":["czech republic","czechia"],"Iceland":["iceland"],"Spain":["spain"],"South Korea":["south korea","korea"],"Sweden":["sweden"],"Switzerland":["switzerland"],"Australia":["australia"],"Israel":["israel"],"Singapore":["singapore"],"Denmark":["denmark"],"Finland":["finland"],"Belgium":["belgium"],"Austria":["austria"],"Ireland":["ireland"],"Poland":["poland"],"Turkey":["turkey","türkiye"],"Argentina":["argentina"],"Chile":["chile"],"Colombia":["colombia"],"Mexico":["mexico"],"Thailand":["thailand"],"Indonesia":["indonesia"],"Malaysia":["malaysia"],"Philippines":["philippines"],"Vietnam":["vietnam"],"Taiwan":["taiwan"],"Hong Kong":["hong kong"],"New Zealand":["new zealand"],"South Africa":["south africa"],"Nigeria":["nigeria"],"Kenya":["kenya"],"Egypt":["egypt"],"UAE":["united arab emirates"],"Romania":["romania"],"Hungary":["hungary"],"Greece":["greece"],"Croatia":["croatia"],"Estonia":["estonia"],"Latvia":["latvia"],"Lithuania":["lithuania"],"Slovakia":["slovakia"],"Slovenia":["slovenia"],"Bulgaria":["bulgaria"],"Serbia":["serbia"],"Ukraine":["ukraine"],"Belarus":["belarus"],"Georgia":["georgia"],"Kazakhstan":["kazakhstan"]}

REGION_MAP = {"Bavaria":["bayern","bavaria"],"Aichi":["aichi"],"Jiangsu":["jiangsu"],"Tennessee":["tennessee"],"Delhi":["delhi","new delhi"],"California":["california"],"Gifu":["gifu"],"Maharashtra":["maharashtra"],"Île-de-France":["ile-de-france","île-de-france"],"Greater London":["greater london","london"],"Ontario":["ontario"],"São Paulo":["são paulo","sao paulo"],"Campania":["campania"],"North Holland":["north holland","noord-holland"],"South Holland":["south holland","zuid-holland"],"Moscow":["moscow","moskva"],"Makkah":["makkah","mecca"],"Riyadh":["riyadh"],"Oberhaching":["oberhaching"],"Zhejiang":["zhejiang"],"Guangdong":["guangdong"],"Beijing":["beijing"],"Shanghai":["shanghai"],"Tokyo":["tokyo"],"Osaka":["osaka"],"Kanagawa":["kanagawa"],"Saitama":["saitama"],"Chiba":["chiba"],"Hyogo":["hyogo"],"Hokkaido":["hokkaido"],"Fukuoka":["fukuoka"],"Kyoto":["kyoto"],"Shizuoka":["shizuoka"],"Hiroshima":["hiroshima"],"New York":["new york"],"Texas":["texas"],"Florida":["florida"],"Illinois":["illinois"],"Pennsylvania":["pennsylvania"],"Ohio":["ohio"],"North Carolina":["north carolina"],"Michigan":["michigan"],"New Jersey":["new jersey"],"Virginia":["virginia"],"Washington":["washington"],"Arizona":["arizona"],"Massachusetts":["massachusetts"],"Colorado":["colorado"],"Minnesota":["minnesota"],"Oregon":["oregon"],"Utah":["utah"],"Connecticut":["connecticut"],"Maryland":["maryland"],"Indiana":["indiana"],"Wisconsin":["wisconsin"],"Missouri":["missouri"],"Nordrhein-Westfalen":["nordrhein-westfalen","north rhine-westphalia"],"Baden-Württemberg":["baden-württemberg","baden-wurttemberg"],"Hessen":["hessen","hesse"],"Niedersachsen":["niedersachsen","lower saxony"],"Berlin":["berlin"],"Hamburg":["hamburg"],"Sachsen":["sachsen","saxony"],"Schleswig-Holstein":["schleswig-holstein"],"Brandenburg":["brandenburg"],"Rheinland-Pfalz":["rheinland-pfalz","rhineland-palatinate"],"North Region":["norte","north"],"South East England":["south east","oxfordshire","surrey","kent","hampshire","berkshire","east sussex","west sussex"],"Georgia":["georgia"],"Kentucky":["kentucky"],"Louisiana":["louisiana"],"Alabama":["alabama"],"South Carolina":["south carolina"],"Delaware":["delaware"]}


def matches_country(desc, country):
    dl = desc.lower()
    aliases = COUNTRY_MAP.get(country, [country.lower()])
    return any(a in dl for a in aliases)


def matches_region(desc, region):
    dl = desc.lower()
    aliases = REGION_MAP.get(region, [region.lower()])
    return any(a in dl for a in aliases)


def scrape_city(city_data, cookies):
    """Scrape a single city. Returns result dict."""
    row = city_data["row"]
    city = city_data["city"]
    region = city_data["region"]
    country = city_data["country"]
    nc = normalize_city(city)

    queries = [f"{nc}, {region}, {country}", f"{nc}, {country}", nc]
    best = None
    mq = "none"

    for q in queries:
        encoded = urllib.parse.quote(q)
        data = api_get(f"/v4/data/autocompletes?query={encoded}&collection_ids=location.cities&limit=10", cookies)
        entities = data.get("entities", [])
        if not entities:
            continue

        # Priority 1: exact country + region match
        for e in entities:
            desc = e.get("short_description", "")
            if matches_country(desc, country) and matches_region(desc, region):
                best = e
                mq = "exact"
                break
        if best:
            break

        # Priority 2: country match only
        for e in entities:
            desc = e.get("short_description", "")
            if matches_country(desc, country):
                if not best or mq != "country":
                    best = e
                    mq = "country"
        if mq == "country":
            break

    if not best:
        return {"row": row, "city": city, "region": region, "country": country,
                "count": 0, "status": "not_found", "match_quality": "none"}

    loc_id = best["identifier"]
    loc_desc = best.get("short_description", "")
    uuid = loc_id["uuid"]

    search_body = {
        "field_ids": ["identifier"],
        "order": [{"field_id": "rank_principal_investor", "sort": "asc"}],
        "query": [
            {"type": "predicate", "field_id": "investor_type", "operator_id": "includes", "values": ["angel"]},
            {"type": "predicate", "field_id": "location_identifiers", "operator_id": "includes", "values": [uuid]},
        ],
        "limit": 1,
    }
    sd = api_post("/v4/data/searches/principal.investors", search_body, cookies)

    return {"row": row, "city": city, "region": region, "country": country,
            "count": sd.get("count", 0), "status": "ok",
            "location_matched": loc_desc, "match_quality": mq, "uuid": uuid}


def load_cities():
    cities = []
    with open(SOURCE_CSV, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            if not row or not row[0].strip():
                continue
            try:
                row_num = int(row[0])
            except ValueError:
                continue
            if ROW_START <= row_num <= ROW_END:
                cities.append({"row": row_num, "city": row[2], "region": row[3], "country": row[4]})
    return cities


def load_existing():
    if RESULTS_FILE.exists():
        with open(RESULTS_FILE) as f:
            data = json.load(f)
        return {r["row"]: r for r in data if r.get("status") != "error"}
    return {}


def save_results(by_row):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    merged = sorted(by_row.values(), key=lambda x: x["row"])
    with open(RESULTS_FILE, "w", encoding="utf-8") as f:
        json.dump(merged, f, ensure_ascii=False, indent=2)


def main():
    print("Crunchbase scraper (direct HTTP, no browser)")

    cookies = load_cookies_from_state()
    print(f"Loaded cookies ({len(cookies)} chars)")

    # Quick test
    try:
        test = api_get("/v4/data/autocompletes?query=Munich&collection_ids=location.cities&limit=1", cookies)
        if test.get("entities"):
            print(f"API test OK: {test['entities'][0].get('short_description', '?')}")
        else:
            print("API test: no results (cookies may be invalid)")
            sys.exit(1)
    except Exception as e:
        print(f"API test FAILED: {e}")
        print("Cookies expired. Re-login locally and upload fresh browser_state.json")
        sys.exit(1)

    cities = load_cities()
    existing = load_existing()
    remaining = [c for c in cities if c["row"] not in existing]
    print(f"Total: {len(cities)}, existing OK: {len(existing)}, remaining: {len(remaining)}")

    if not remaining:
        print("All done!")
        return

    consecutive_errors = 0
    for i, city_data in enumerate(remaining):
        try:
            result = scrape_city(city_data, cookies)
            existing[result["row"]] = result

            icon = "+" if result["status"] == "ok" else "?" if result["status"] == "not_found" else "!"
            print(f"  [{i+1}/{len(remaining)}] {icon} Row {result['row']}: {result['city']} -> {result.get('count', '?')} ({result.get('match_quality', '?')})")

            consecutive_errors = 0

        except Exception as e:
            err_msg = str(e)
            print(f"  [{i+1}/{len(remaining)}] ! Row {city_data['row']}: {city_data['city']} ERROR: {err_msg}")
            existing[city_data["row"]] = {
                "row": city_data["row"], "city": city_data["city"],
                "region": city_data["region"], "country": city_data["country"],
                "count": -1, "status": "error", "error": err_msg,
            }
            consecutive_errors += 1

            if consecutive_errors >= 3:
                print(f"\n  3 consecutive errors — session likely expired. Stopping.")
                save_results(existing)
                sys.exit(2)

        # Save every 25 cities
        if (i + 1) % 25 == 0:
            save_results(existing)
            print(f"  --- Saved {len(existing)} results ---")

        delay = random.uniform(2.5, 3.5)
        time.sleep(delay)

    save_results(existing)

    ok = sum(1 for r in existing.values() if r.get("status") == "ok")
    nf = sum(1 for r in existing.values() if r.get("status") == "not_found")
    err = sum(1 for r in existing.values() if r.get("status") == "error")
    print(f"\n=== DONE ===")
    print(f"Total: {len(existing)} | OK: {ok} | Not found: {nf} | Errors: {err}")


if __name__ == "__main__":
    main()
