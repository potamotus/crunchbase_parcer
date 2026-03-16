#!/usr/bin/env python3
"""
Full automation script: reads batches, generates JS for Playwright execution,
collects and merges results.

This file is used internally — the actual scraping happens via Playwright MCP.
"""

import csv
import json
from pathlib import Path

SOURCE_CSV = Path(__file__).parent / "data" / "source.csv"
OUTPUT_DIR = Path(__file__).parent / "output"
RESULTS_FILE = OUTPUT_DIR / "results.json"

ROW_START = 532
ROW_END = 797
BATCH_SIZE = 25

# The JS scraping function template with improved region matching
SCRAPE_JS_TEMPLATE = '''
async (cities) => {{
    const DELAY = 1200;
    const results = [];

    const countryMap = {{
        "USA": ["united states"],
        "United Kingdom": ["united kingdom"],
        "Germany": ["germany"],
        "Japan": ["japan"],
        "China": ["china"],
        "India": ["india"],
        "France": ["france"],
        "Canada": ["canada"],
        "Brazil": ["brazil"],
        "Italy": ["italy"],
        "Netherlands": ["netherlands"],
        "Russia": ["russia"],
        "Norway": ["norway"],
        "Portugal": ["portugal"],
        "Saudi Arabia": ["saudi arabia"],
        "Czech Republic": ["czech republic", "czechia"],
        "Iceland": ["iceland"]
    }};

    // Region name aliases (CSV name -> possible Crunchbase names)
    const regionMap = {{
        "Bavaria": ["bayern", "bavaria"],
        "Aichi": ["aichi"],
        "Jiangsu": ["jiangsu"],
        "Tennessee": ["tennessee"],
        "Delhi": ["delhi"],
        "California": ["california"],
        "Gifu": ["gifu"],
        "Maharashtra": ["maharashtra"],
        "Île-de-France": ["ile-de-france", "île-de-france"],
        "Greater London": ["greater london", "london"],
        "Ontario": ["ontario"],
        "São Paulo": ["são paulo", "sao paulo"],
        "Campania": ["campania"],
        "North Holland": ["north holland", "noord-holland"],
        "South Holland": ["south holland", "zuid-holland"],
        "Moscow": ["moscow", "moskva"],
        "Makkah": ["makkah", "mecca"],
        "Riyadh": ["riyadh"],
    }};

    function matchesCountry(desc, country) {{
        const d = desc.toLowerCase();
        const aliases = countryMap[country] || [country.toLowerCase()];
        return aliases.some(a => d.includes(a));
    }}

    function matchesRegion(desc, region) {{
        const d = desc.toLowerCase();
        const aliases = regionMap[region] || [region.toLowerCase()];
        return aliases.some(a => d.includes(a));
    }}

    function sleep(ms) {{ return new Promise(r => setTimeout(r, ms)); }}

    function normalizeCity(city) {{
        // Remove special chars, macrons etc for search
        return city
            .replace(/[Ōō]/g, 'O')
            .replace(/[āǎ]/g, 'a')
            .replace(/[ūǔ]/g, 'u')
            .replace(/[éè]/g, 'e')
            .replace(/[ãâ]/g, 'a')
            .replace(/\\s*\\(.*?\\)\\s*/g, ''); // Remove parenthetical like "(USA)"
    }}

    for (let i = 0; i < cities.length; i++) {{
        const {{ row, city, region, country }} = cities[i];
        const normCity = normalizeCity(city);

        try {{
            // Try queries in order of specificity
            const queries = [
                `${{normCity}}, ${{region}}, ${{country}}`,
                `${{normCity}}, ${{country}}`,
                normCity
            ];

            let bestMatch = null;
            let matchQuality = "none";

            for (const q of queries) {{
                const r = await fetch(
                    `/v4/data/autocompletes?query=${{encodeURIComponent(q)}}&collection_ids=location.cities&limit=10`,
                    {{ credentials: 'include' }}
                );
                const data = await r.json();
                if (!data.entities || data.entities.length === 0) continue;

                // Priority 1: exact country + region match
                for (const e of data.entities) {{
                    const desc = e.short_description || '';
                    if (matchesCountry(desc, country) && matchesRegion(desc, region)) {{
                        bestMatch = e;
                        matchQuality = "exact";
                        break;
                    }}
                }}
                if (bestMatch) break;

                // Priority 2: country match only
                for (const e of data.entities) {{
                    const desc = e.short_description || '';
                    if (matchesCountry(desc, country)) {{
                        if (!bestMatch || matchQuality !== "country") {{
                            bestMatch = e;
                            matchQuality = "country";
                        }}
                    }}
                }}
                if (matchQuality === "country") break;
            }}

            if (!bestMatch) {{
                results.push({{ row, city, region, country, count: 0, status: "not_found", location_matched: null, match_quality: "none" }});
                await sleep(DELAY);
                continue;
            }}

            const locId = bestMatch.identifier;
            const locDesc = bestMatch.short_description || '';

            // Search for angel investors
            const sr = await fetch('/v4/data/searches/principal.investors', {{
                method: 'POST',
                headers: {{ 'Content-Type': 'application/json' }},
                credentials: 'include',
                body: JSON.stringify({{
                    field_ids: ["identifier"],
                    order: [{{ field_id: "rank_principal_investor", sort: "asc" }}],
                    query: [
                        {{ type: "predicate", field_id: "investor_type", operator_id: "includes", values: ["angel"] }},
                        {{ type: "predicate", field_id: "location_identifiers", operator_id: "includes", values: [locId.uuid] }}
                    ],
                    limit: 1
                }})
            }});
            const searchData = await sr.json();

            results.push({{
                row, city, region, country,
                count: searchData.count || 0,
                status: "ok",
                location_matched: locDesc,
                match_quality: matchQuality,
                uuid: locId.uuid
            }});

        }} catch (err) {{
            results.push({{ row, city, region, country, count: -1, status: "error", error: err.message }});
        }}

        await sleep(DELAY);
    }}

    return results;
}}
'''


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
                cities.append({
                    "row": row_num,
                    "city": row[2],
                    "region": row[3],
                    "country": row[4],
                })
    return cities


def make_batches(cities):
    return [cities[i:i+BATCH_SIZE] for i in range(0, len(cities), BATCH_SIZE)]


def merge_results(existing_file, new_results):
    """Merge new results into existing results file."""
    existing = []
    if existing_file.exists():
        with open(existing_file) as f:
            existing = json.load(f)

    # Build index of existing by row
    by_row = {r["row"]: r for r in existing}
    for r in new_results:
        by_row[r["row"]] = r

    merged = sorted(by_row.values(), key=lambda x: x["row"])

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(existing_file, "w", encoding="utf-8") as f:
        json.dump(merged, f, ensure_ascii=False, indent=2)

    return merged


if __name__ == "__main__":
    cities = load_cities()
    batches = make_batches(cities)
    print(f"Total: {len(cities)} cities in {len(batches)} batches")
    for i, b in enumerate(batches):
        print(f"  Batch {i}: {json.dumps(b, ensure_ascii=False)}")
