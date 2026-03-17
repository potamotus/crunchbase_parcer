#!/usr/bin/env python3
"""Fix cities that were mismatched (e.g. Lille->Paris)."""
import json
import time
from playwright.sync_api import sync_playwright

FIXES = [
    {"row": 1008, "city": "Lille", "query": "Lille, Hauts-de-France"},
    {"row": 1012, "city": "Roubaix", "query": "Roubaix, Nord, France"},
    {"row": 1021, "city": "Tourcoing", "query": "Tourcoing, Nord, France"},
    {"row": 1022, "city": "Fretin", "query": "Fretin, Nord, France"},
    {"row": 885, "city": "Chuo", "query": "Chuo, Tokyo, Japan"},
    {"row": 901, "city": "Ota", "query": "Ota, Tokyo, Japan"},
    {"row": 905, "city": "Bunkyo", "query": "Bunkyo, Tokyo, Japan"},
    {"row": 1041, "city": "Troisvierges", "query": "Troisvierges, Luxembourg"},
    {"row": 1000, "city": "Daffodil", "query": "Daffodil, Dhaka, Bangladesh"},
]

FIX_JS = r"""
async (fixes) => {
    const sleep = ms => new Promise(r => setTimeout(r, ms));
    const results = [];
    for (const fix of fixes) {
        try {
            const r = await fetch("/v4/data/autocompletes?query=" + encodeURIComponent(fix.query) + "&collection_ids=location.cities&limit=5", {credentials:"include"});
            const data = await r.json();
            const entities = data.entities || [];

            let matched = null;
            for (const e of entities) {
                const name = (e.identifier && e.identifier.value || "").toLowerCase();
                if (name.includes(fix.city.toLowerCase())) {
                    matched = e;
                    break;
                }
            }
            if (!matched && entities.length > 0) matched = entities[0];

            if (!matched) {
                results.push({row: fix.row, city: fix.city, count: 0, status: "not_found", match_quality: "manual_not_found"});
                await sleep(3000);
                continue;
            }

            const sr = await fetch("/v4/data/searches/principal.investors", {
                method: "POST", headers: {"Content-Type": "application/json"}, credentials: "include",
                body: JSON.stringify({field_ids:["identifier"], order:[{field_id:"rank_principal_investor",sort:"asc"}],
                    query:[{type:"predicate",field_id:"investor_type",operator_id:"includes",values:["angel"]},
                           {type:"predicate",field_id:"location_identifiers",operator_id:"includes",values:[matched.identifier.uuid]}], limit:1})
            });
            const sd = await sr.json();
            results.push({row: fix.row, city: fix.city, count: sd.count||0, status: "ok",
                location_matched: matched.short_description, match_quality: "manual_fix", uuid: matched.identifier.uuid});
        } catch(e) {
            results.push({row: fix.row, city: fix.city, count: -1, status: "error", error: e.message});
        }
        await sleep(3000);
    }
    return results;
}
"""

with sync_playwright() as p:
    browser = p.chromium.launch(headless=False)
    ctx = browser.new_context(storage_state="output/browser_state.json")
    page = ctx.new_page()
    page.goto("https://www.crunchbase.com/home", wait_until="networkidle")
    time.sleep(2)

    fixed = page.evaluate(FIX_JS, FIXES)
    for r in fixed:
        print(f"  Row {r['row']}: {r['city']} -> {r.get('location_matched','?')} count={r['count']}")

    with open("output/results.json") as f:
        all_data = json.load(f)
    by_row = {r["row"]: r for r in all_data}
    for r in fixed:
        old = by_row.get(r["row"], {})
        r["region"] = old.get("region", "")
        r["country"] = old.get("country", "")
        by_row[r["row"]] = r
    merged = sorted(by_row.values(), key=lambda x: x["row"])
    with open("output/results.json", "w") as f:
        json.dump(merged, f, ensure_ascii=False, indent=2)
    print(f"Saved {len(merged)} results")
    browser.close()
