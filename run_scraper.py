#!/usr/bin/env python3
"""
Automated Crunchbase scraper using Playwright.
Logs into Crunchbase, then queries the internal API for angel investor counts per city.

Usage:
    python3 run_scraper.py --email EMAIL --password PASSWORD [--headless]
"""

import argparse
import csv
import json
import time
import random
import sys
from pathlib import Path

from playwright.sync_api import sync_playwright

PROJECT_DIR = Path(__file__).parent
SOURCE_CSV = PROJECT_DIR / "data" / "source.csv"
OUTPUT_DIR = PROJECT_DIR / "output"
RESULTS_FILE = OUTPUT_DIR / "results.json"
STATE_FILE = OUTPUT_DIR / "browser_state.json"

ROW_START = 532
ROW_END = 1059
BATCH_SIZE = 25

SCRAPE_JS = """
async (cities) => {
    const DELAY = 2500;
    const results = [];
    const cMap = {"USA":["united states"],"United Kingdom":["united kingdom"],"Germany":["germany"],"Japan":["japan"],"China":["china"],"India":["india"],"France":["france"],"Canada":["canada"],"Brazil":["brazil"],"Italy":["italy"],"Netherlands":["netherlands"],"Russia":["russia"],"Norway":["norway"],"Portugal":["portugal"],"Saudi Arabia":["saudi arabia"],"Czech Republic":["czech republic","czechia"],"Iceland":["iceland"],"Spain":["spain"],"South Korea":["south korea","korea"],"Sweden":["sweden"],"Switzerland":["switzerland"],"Australia":["australia"],"Israel":["israel"],"Singapore":["singapore"],"Denmark":["denmark"],"Finland":["finland"],"Belgium":["belgium"],"Austria":["austria"],"Ireland":["ireland"],"Poland":["poland"],"Turkey":["turkey","türkiye"],"Argentina":["argentina"],"Chile":["chile"],"Colombia":["colombia"],"Mexico":["mexico"],"Thailand":["thailand"],"Indonesia":["indonesia"],"Malaysia":["malaysia"],"Philippines":["philippines"],"Vietnam":["vietnam"],"Taiwan":["taiwan"],"Hong Kong":["hong kong"],"New Zealand":["new zealand"],"South Africa":["south africa"],"Nigeria":["nigeria"],"Kenya":["kenya"],"Egypt":["egypt"],"UAE":["united arab emirates"],"Romania":["romania"],"Hungary":["hungary"],"Greece":["greece"],"Croatia":["croatia"],"Estonia":["estonia"],"Latvia":["latvia"],"Lithuania":["lithuania"],"Slovakia":["slovakia"],"Slovenia":["slovenia"],"Bulgaria":["bulgaria"],"Serbia":["serbia"],"Ukraine":["ukraine"],"Belarus":["belarus"],"Georgia":["georgia"],"Kazakhstan":["kazakhstan"],"Uzbekistan":["uzbekistan"],"Pakistan":["pakistan"],"Bangladesh":["bangladesh"],"Sri Lanka":["sri lanka"],"Myanmar":["myanmar"],"Cambodia":["cambodia"],"Laos":["laos"],"Mongolia":["mongolia"],"Nepal":["nepal"],"Luxembourg":["luxembourg"],"Liechtenstein":["liechtenstein"],"Monaco":["monaco"],"Malta":["malta"],"Cyprus":["cyprus"],"Jordan":["jordan"],"Lebanon":["lebanon"],"Qatar":["qatar"],"Kuwait":["kuwait"],"Bahrain":["bahrain"],"Oman":["oman"],"Morocco":["morocco"],"Tunisia":["tunisia"],"Algeria":["algeria"],"Ghana":["ghana"],"Ethiopia":["ethiopia"],"Tanzania":["tanzania"],"Uganda":["uganda"],"Rwanda":["rwanda"],"Senegal":["senegal"],"Ivory Coast":["ivory coast","côte d'ivoire"],"Peru":["peru"],"Ecuador":["ecuador"],"Uruguay":["uruguay"],"Paraguay":["paraguay"],"Bolivia":["bolivia"],"Venezuela":["venezuela"],"Panama":["panama"],"Costa Rica":["costa rica"],"Guatemala":["guatemala"],"Dominican Republic":["dominican republic"],"Puerto Rico":["puerto rico"],"Jamaica":["jamaica"],"Trinidad and Tobago":["trinidad and tobago"]};
    const rMap = {"Bavaria":["bayern","bavaria"],"Aichi":["aichi"],"Jiangsu":["jiangsu"],"Tennessee":["tennessee"],"Delhi":["delhi","new delhi"],"California":["california"],"Gifu":["gifu"],"Maharashtra":["maharashtra"],"Île-de-France":["ile-de-france","île-de-france"],"Greater London":["greater london","london"],"Ontario":["ontario"],"São Paulo":["são paulo","sao paulo"],"Campania":["campania"],"North Holland":["north holland","noord-holland"],"South Holland":["south holland","zuid-holland"],"Moscow":["moscow","moskva"],"Makkah":["makkah","mecca"],"Riyadh":["riyadh"],"Oberhaching":["oberhaching"],"Zhejiang":["zhejiang"],"Guangdong":["guangdong"],"Beijing":["beijing"],"Shanghai":["shanghai"],"Hubei":["hubei"],"Sichuan":["sichuan"],"Henan":["henan"],"Hunan":["hunan"],"Fujian":["fujian"],"Anhui":["anhui"],"Hebei":["hebei"],"Liaoning":["liaoning"],"Shandong":["shandong"],"Heilongjiang":["heilongjiang"],"Jilin":["jilin"],"Shaanxi":["shaanxi"],"Chongqing":["chongqing"],"Tianjin":["tianjin"],"Guizhou":["guizhou"],"Yunnan":["yunnan"],"Gansu":["gansu"],"Inner Mongolia":["inner mongolia"],"Ningxia":["ningxia"],"Xinjiang":["xinjiang"],"Tibet":["tibet"],"Hainan":["hainan"],"Jiangxi":["jiangxi"],"Shanxi":["shanxi"],"Guangxi":["guangxi"],"Qinghai":["qinghai"],"Tokyo":["tokyo"],"Osaka":["osaka"],"Kanagawa":["kanagawa"],"Saitama":["saitama"],"Chiba":["chiba"],"Hyogo":["hyogo"],"Hokkaido":["hokkaido"],"Fukuoka":["fukuoka"],"Kyoto":["kyoto"],"Shizuoka":["shizuoka"],"Hiroshima":["hiroshima"],"Ibaraki":["ibaraki"],"Miyagi":["miyagi"],"Niigata":["niigata"],"Nagano":["nagano"],"Tochigi":["tochigi"],"Gunma":["gunma"],"Mie":["mie"],"Nara":["nara"],"Okayama":["okayama"],"Kumamoto":["kumamoto"],"Kagoshima":["kagoshima"],"Okinawa":["okinawa"],"Shiga":["shiga"],"Toyama":["toyama"],"Ishikawa":["ishikawa"],"Fukui":["fukui"],"Yamanashi":["yamanashi"],"Wakayama":["wakayama"],"Tokushima":["tokushima"],"Kagawa":["kagawa"],"Ehime":["ehime"],"Kochi":["kochi"],"Saga":["saga"],"Nagasaki":["nagasaki"],"Oita":["oita"],"Miyazaki":["miyazaki"],"Iwate":["iwate"],"Akita":["akita"],"Yamagata":["yamagata"],"Fukushima":["fukushima"],"Tottori":["tottori"],"Shimane":["shimane"],"Yamaguchi":["yamaguchi"],"New York":["new york"],"Texas":["texas"],"Florida":["florida"],"Illinois":["illinois"],"Pennsylvania":["pennsylvania"],"Ohio":["ohio"],"Georgia":["georgia"],"North Carolina":["north carolina"],"Michigan":["michigan"],"New Jersey":["new jersey"],"Virginia":["virginia"],"Washington":["washington"],"Arizona":["arizona"],"Massachusetts":["massachusetts"],"Colorado":["colorado"],"Minnesota":["minnesota"],"Wisconsin":["wisconsin"],"Missouri":["missouri"],"Maryland":["maryland"],"Indiana":["indiana"],"Connecticut":["connecticut"],"Oregon":["oregon"],"Oklahoma":["oklahoma"],"Kentucky":["kentucky"],"Louisiana":["louisiana"],"Alabama":["alabama"],"South Carolina":["south carolina"],"Utah":["utah"],"Iowa":["iowa"],"Arkansas":["arkansas"],"Nevada":["nevada"],"Mississippi":["mississippi"],"Kansas":["kansas"],"Nebraska":["nebraska"],"Idaho":["idaho"],"Hawaii":["hawaii"],"New Hampshire":["new hampshire"],"Maine":["maine"],"Montana":["montana"],"Rhode Island":["rhode island"],"Delaware":["delaware"],"South Dakota":["south dakota"],"North Dakota":["north dakota"],"Alaska":["alaska"],"Vermont":["vermont"],"Wyoming":["wyoming"],"West Virginia":["west virginia"],"District of Columbia":["district of columbia","d.c."],"Hessen":["hessen","hesse"],"Baden-Württemberg":["baden-württemberg","baden-wurttemberg"],"Nordrhein-Westfalen":["nordrhein-westfalen","north rhine-westphalia"],"Niedersachsen":["niedersachsen","lower saxony"],"Sachsen":["sachsen","saxony"],"Rheinland-Pfalz":["rheinland-pfalz","rhineland-palatinate"],"Schleswig-Holstein":["schleswig-holstein"],"Brandenburg":["brandenburg"],"Sachsen-Anhalt":["sachsen-anhalt","saxony-anhalt"],"Thüringen":["thüringen","thuringia"],"Mecklenburg-Vorpommern":["mecklenburg-vorpommern"],"Saarland":["saarland"],"Bremen":["bremen"],"Hamburg":["hamburg"],"Berlin":["berlin"]};
    const mC = (d,c) => {const dl=d.toLowerCase();return(cMap[c]||[c.toLowerCase()]).some(a=>dl.includes(a));};
    const mR = (d,r) => {const dl=d.toLowerCase();return(rMap[r]||[r.toLowerCase()]).some(a=>dl.includes(a));};
    const sleep = ms => new Promise(r => setTimeout(r, ms));
    const norm = c => c.replace(/[Ōō]/g,'O').replace(/[āǎ]/g,'a').replace(/[ūǔ]/g,'u').replace(/[éèê]/g,'e').replace(/[ãâà]/g,'a').replace(/[íì]/g,'i').replace(/[óòô]/g,'o').replace(/[úù]/g,'u').replace(/[ñ]/g,'n').replace(/[ç]/g,'c').replace(/\\s*\\(.*?\\)\\s*/g,'');

    for (let i = 0; i < cities.length; i++) {
        const {row, city, region, country} = cities[i];
        const nc = norm(city);
        try {
            const queries = [`${nc}, ${region}, ${country}`, `${nc}, ${country}`, nc];
            let best = null, mq = "none";
            for (const q of queries) {
                const r = await fetch(`/v4/data/autocompletes?query=${encodeURIComponent(q)}&collection_ids=location.cities&limit=10`, {credentials:'include'});
                if (!r.ok) throw new Error(`HTTP ${r.status}`);
                const data = await r.json();
                if (!data.entities || data.entities.length === 0) continue;
                for (const e of data.entities) {
                    const desc = e.short_description || '';
                    if (mC(desc, country) && mR(desc, region)) { best = e; mq = "exact"; break; }
                }
                if (best) break;
                for (const e of data.entities) {
                    const desc = e.short_description || '';
                    if (mC(desc, country)) { if (!best || mq !== "country") { best = e; mq = "country"; } }
                }
                if (mq === "country") break;
            }
            if (!best) {
                results.push({row, city, region, country, count: 0, status: "not_found", match_quality: "none"});
                await sleep(DELAY + Math.random()*1000);
                continue;
            }
            const locId = best.identifier;
            const locDesc = best.short_description || '';
            const sr = await fetch('/v4/data/searches/principal.investors', {
                method: 'POST', headers: {'Content-Type': 'application/json'}, credentials: 'include',
                body: JSON.stringify({field_ids:["identifier"], order:[{field_id:"rank_principal_investor",sort:"asc"}],
                    query:[{type:"predicate",field_id:"investor_type",operator_id:"includes",values:["angel"]},
                           {type:"predicate",field_id:"location_identifiers",operator_id:"includes",values:[locId.uuid]}], limit:1})
            });
            if (!sr.ok) throw new Error(`Search HTTP ${sr.status}`);
            const sd = await sr.json();
            results.push({row, city, region, country, count: sd.count||0, status:"ok", location_matched: locDesc, match_quality: mq, uuid: locId.uuid});
        } catch(err) {
            results.push({row, city, region, country, count: -1, status: "error", error: err.message});
        }
        await sleep(DELAY + Math.random()*1000);
    }
    return results;
}
"""


def load_cities() -> list[dict]:
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
                cities.append({
                    "row": row_num,
                    "city": row[2],
                    "region": row[3],
                    "country": row[4],
                })
    return cities


def load_existing_results() -> dict:
    if RESULTS_FILE.exists():
        with open(RESULTS_FILE) as f:
            data = json.load(f)
        return {r["row"]: r for r in data}
    return {}


def save_results(by_row: dict):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    merged = sorted(by_row.values(), key=lambda x: x["row"])
    with open(RESULTS_FILE, "w", encoding="utf-8") as f:
        json.dump(merged, f, ensure_ascii=False, indent=2)
    return merged


def do_login(page, email, password):
    """Login to Crunchbase. Returns True on success."""
    print("Logging into Crunchbase...")
    page.goto("https://www.crunchbase.com/login", wait_until="networkidle")
    time.sleep(3)
    page.fill('input[name="email"], input[type="email"]', email)
    page.fill('input[name="password"], input[type="password"]', password)
    page.click('button[type="submit"]')
    page.wait_for_url("**/home**", timeout=30000)
    print("Login successful!")
    return True


def main():
    parser = argparse.ArgumentParser(description="Crunchbase scraper via Playwright")
    parser.add_argument("--email", help="Crunchbase email (for local login)")
    parser.add_argument("--password", help="Crunchbase password (for local login)")
    parser.add_argument("--resume", action="store_true", help="Use saved browser_state.json (no login)")
    parser.add_argument("--headless", action="store_true", help="Run headless")
    args = parser.parse_args()

    if not args.resume and (not args.email or not args.password):
        print("Error: --email and --password required, or use --resume with browser_state.json")
        sys.exit(1)

    cities = load_cities()
    batches = [cities[i:i+BATCH_SIZE] for i in range(0, len(cities), BATCH_SIZE)]
    print(f"Loaded {len(cities)} cities in {len(batches)} batches")

    existing = load_existing_results()
    # Remove any error results so they get retried
    existing = {k: v for k, v in existing.items() if v.get("status") != "error"}
    print(f"Existing OK results: {len(existing)} cities")

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=args.headless,
            args=["--disable-blink-features=AutomationControlled", "--no-sandbox"]
        )

        if args.resume and STATE_FILE.exists():
            context = browser.new_context(
                storage_state=str(STATE_FILE),
                user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            )
            print("Restored browser state from cookies")
        else:
            context = browser.new_context(
                user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            )

        page = context.new_page()
        page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

        if not args.resume:
            do_login(page, args.email, args.password)
            OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
            context.storage_state(path=str(STATE_FILE))
        else:
            page.goto("https://www.crunchbase.com/home", wait_until="networkidle")
            time.sleep(2)

        for batch_idx in range(len(batches)):
            batch = batches[batch_idx]

            # Skip fully completed batches
            batch_rows = {c["row"] for c in batch}
            if batch_rows.issubset(existing.keys()):
                ok_count = sum(1 for r in batch_rows if existing[r].get("status") == "ok")
                if ok_count == len(batch):
                    print(f"Batch {batch_idx} already complete, skipping")
                    continue

            # Only send cities that don't have OK results yet
            cities_to_scrape = [c for c in batch if c["row"] not in existing or existing[c["row"]].get("status") != "ok"]

            print(f"\n=== Batch {batch_idx}/{len(batches)-1}: {len(cities_to_scrape)} cities (rows {batch[0]['row']}-{batch[-1]['row']}) ===")

            try:
                result = page.evaluate(SCRAPE_JS, cities_to_scrape)

                errors_in_batch = 0
                for r in result:
                    existing[r["row"]] = r
                    status_icon = "+" if r["status"] == "ok" else "?" if r["status"] == "not_found" else "!"
                    print(f"  {status_icon} Row {r['row']}: {r['city']} -> {r.get('count', '?')} ({r.get('match_quality', '?')})")
                    if r["status"] == "error":
                        errors_in_batch += 1

                save_results(existing)
                print(f"  Saved {len(existing)} results total")

                # If too many errors, session expired — stop
                if errors_in_batch > len(cities_to_scrape) // 2:
                    print(f"  SESSION EXPIRED ({errors_in_batch} errors). Need fresh cookies.")
                    # Remove error results so next run retries them
                    for r in result:
                        if r["status"] == "error":
                            existing.pop(r["row"], None)
                    save_results(existing)
                    print("  Stopping. Re-run local login and upload fresh browser_state.json")
                    browser.close()
                    sys.exit(2)

            except Exception as e:
                print(f"  EXCEPTION in batch {batch_idx}: {e}")
                print("  Stopping.")
                browser.close()
                sys.exit(1)

            # Delay between batches
            if batch_idx < len(batches) - 1:
                delay = random.uniform(5, 10)
                print(f"  Waiting {delay:.1f}s before next batch...")
                time.sleep(delay)

        # Final save
        all_results = save_results(existing)

        # Summary
        ok = sum(1 for r in all_results if r.get("status") == "ok")
        nf = sum(1 for r in all_results if r.get("status") == "not_found")
        err = sum(1 for r in all_results if r.get("status") == "error")
        print(f"\n=== DONE ===")
        print(f"Total: {len(all_results)} cities")
        print(f"  OK: {ok}")
        print(f"  Not found: {nf}")
        print(f"  Errors: {err}")
        print(f"Results: {RESULTS_FILE}")

        browser.close()


if __name__ == "__main__":
    main()
