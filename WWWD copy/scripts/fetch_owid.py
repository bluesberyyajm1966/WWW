"""
fetch_owid.py
Fetches Our World in Data datasets.

STRATEGY:
  - Energy data: single bulk CSV from OWID's GitHub
    https://owid-public.owid.io/data/energy/owid-energy-data.csv
    (all energy/electricity/emissions fields in one download — no more 404s)

  - Everything else: OWID grapher chart CSVs (no ?v=1)

Run from your WWWD folder:
    python3 fetch_owid.py

Output: data/owid/owid_data.json
"""

import requests, json, os, csv, io, time

OUT_PATH = os.path.join("data", "owid", "owid_data.json")
BASE     = "https://ourworldindata.org/grapher"
HEADERS  = {"User-Agent": "Mozilla/5.0 WorldExplorer/1.0"}

# ── ENERGY FIELDS ──────────────────────────────────────────────────────────
# Downloaded from the OWID bulk energy CSV.
# Column names from: https://github.com/owid/energy-data/blob/master/owid-energy-codebook.csv
ENERGY_FIELDS = {
    "solar_electricity":          "solarElec",
    "wind_electricity":           "windElec",
    "nuclear_electricity":        "nuclearElec",
    "fossil_electricity":         "fossilElec",
    "renewables_electricity":     "renewableElec",
    "hydro_electricity":          "hydroElec",
    "solar_share_elec":           "solarShare",
    "wind_share_elec":            "windShare",
    "nuclear_share_elec":         "nuclearShare",
    "fossil_share_elec":          "fossilShareElec",
    "renewables_share_elec":      "renewableShare",
    "co2":                        "co2",
    "co2_per_capita":             "co2PerCap",
    "co2_per_gdp":                "co2PerGDP",
    "total_ghg":                  "totalGhg",
    "methane":                    "methane",
    "nitrous_oxide":              "nitrousOxide",
    "share_global_co2":           "shareGlobalCo2",
    "coal_co2":                   "coalCo2",
    "oil_co2":                    "oilCo2",
    "gas_co2":                    "gasCo2",
    "cumulative_co2":             "cumulativeCo2",
}

# ── GRAPHER CHART INDICATORS ───────────────────────────────────────────────
# (field_name, [slug1, slug2, ...], value_col_keyword_or_None)
# No ?v=1 — just plain .csv
CHART_INDICATORS = [
    # Health
    ("diabetesRate",      ["diabetes-prevalence"],                         None),
    ("childMortality",    ["child-mortality"],                             None),
    ("lifeExpOwid",       ["life-expectancy"],                             None),
    ("maternMortOwid",    ["maternal-mortality"],                          None),
    ("cancerDeaths",      ["death-rate-from-cancer"],                      None),
    ("suicideRate",       ["suicide-death-rates-by-sex"],                  "Both sexes"),
    ("alcoholConsump",    ["total-alcohol-consumption-per-capita",
                           "alcohol-consumption-per-person"],              None),
    ("homicideRate",      ["homicide-rate"],                               None),
    ("cvdDeaths",         ["death-rate-from-cardiovascular-diseases",
                           "cardiovascular-disease-death-rates-age-standardized"],
                                                                           None),
    ("malariaDeaths",     ["malaria-death-rates",
                           "malaria-death-rate"],                          None),
    ("hivRate",           ["hiv-prevalence",
                           "share-of-population-with-hiv-sdgs"],          None),
    ("smokingDeaths",     ["death-rates-from-smoking",
                           "share-of-deaths-from-smoking"],               None),
    ("airPollutionDeaths",["death-rate-from-air-pollution",
                           "outdoor-air-pollution-deaths"],                None),

    # Society
    ("prisonPop",         ["prison-population",
                           "incarceration-rate",
                           "share-of-population-in-prison"],               None),
    ("roadDeaths",        ["road-deaths",
                           "road-injury-deaths",
                           "death-rates-road"],                            None),
    ("genderWageGap",     ["gender-pay-gap-oecd",
                           "gender-wage-gap-oecd"],                        None),
    ("depressionRate",    ["share-with-depression",
                           "depressive-disorders-prevalence-ihme",
                           "depression-prevalence"],                       None),
    ("corruptionIndex",   ["ti-corruption-perception-index",
                           "corruption-perceptions-index-cpi"],            "Corruption"),
    ("pressFreedom",      ["press-freedom-rsf"],                           "Press Freedom"),
    ("womenInParl",       ["share-of-women-in-parliament",
                           "women-in-parliaments"],                        None),
    ("militarySpend",     ["military-expenditure-as-share-of-gdp"],        None),
    ("touristArrivals",   ["international-tourist-arrivals"],              None),

    # Water & Sanitation
    ("safeWater",         ["share-of-population-with-access-to-clean-water",
                           "access-to-safe-drinking-water",
                           "drinking-water-coverage"],                     None),
    ("safeSanitation",    ["share-of-population-using-safely-managed-sanitation",
                           "safely-managed-sanitation"],                   None),
    ("cleanFuels",        ["access-to-clean-fuels-and-technologies-for-cooking",
                           "share-with-clean-cooking-fuels"],              None),

    # Food & Nature
    ("dailyCalories",     ["daily-per-capita-caloric-supply",
                           "daily-caloric-supply"],                        None),
    ("meatSupply",        ["meat-supply-per-person"],                      None),
    ("forestOwid",        ["forest-area-as-share-of-land-area"],           None),
    ("plasticWaste",      ["plastic-waste-per-capita"],                    None),

    # Education & Development
    ("eduSpendOwid",      ["total-government-expenditure-on-education-gdp",
                           "government-expenditure-on-education-gdp"],     None),
    ("meanSchoolingOwid", ["mean-years-of-schooling"],                     None),
    ("extremePoverty",    ["share-of-population-in-extreme-poverty"],      None),
    ("birthRateOwid",     ["crude-birth-rate"],                            None),
]


# ── HELPERS ────────────────────────────────────────────────────────────────

def fetch_url(url, label=""):
    print(f"  GET {url[:80]}")
    try:
        r = requests.get(url, timeout=120, headers=HEADERS)
        r.raise_for_status()
        text = r.content.decode("utf-8", errors="replace")
        reader = csv.DictReader(io.StringIO(text))
        rows   = list(reader)
        fields = list(reader.fieldnames or [])
        print(f"       {len(rows):,} rows | {len(fields)} cols")
        return rows, fields
    except requests.exceptions.HTTPError as e:
        print(f"       FAILED {e.response.status_code}")
        return [], []
    except Exception as e:
        print(f"       FAILED: {e}")
        return [], []


def find_col(fieldnames, keyword):
    if not keyword:
        return None
    for f in fieldnames:
        if keyword.lower() in f.lower():
            return f
    return None


def auto_val_col(fields):
    skip = {"entity", "code", "year", "annotation", "world region", "note", "country"}
    for f in fields:
        if not any(s in f.lower() for s in skip):
            return f
    return None


def extract_latest(rows, iso_col, year_col, value_col):
    result = {}
    for row in rows:
        iso3 = row.get(iso_col, "").strip().upper()
        if not iso3 or len(iso3) != 3:
            continue
        val_raw = row.get(value_col, "")
        if not val_raw or str(val_raw).strip() in ("", "nan", "None"):
            continue
        try:
            val  = float(val_raw)
            year = str(row.get(year_col, "")).strip()
        except (ValueError, TypeError):
            continue
        if iso3 not in result or year > result[iso3]["year"]:
            result[iso3] = {"value": val, "year": year}
    return result


def store(owid_data, extracted, field_name):
    for iso3, entry in extracted.items():
        owid_data.setdefault(iso3, {})[field_name]           = entry["value"]
        owid_data.setdefault(iso3, {})[field_name + "_year"] = entry["year"]
    return len(extracted)


# ── MAIN ───────────────────────────────────────────────────────────────────

def main():
    print("=" * 65)
    print("  OWID — World Explorer Data Fetcher")
    print("  Energy: bulk CSV from OWID GitHub (no more 404s)")
    print("  Other:  grapher charts (no ?v=1)")
    print("=" * 65)

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)

    if os.path.exists(OUT_PATH):
        with open(OUT_PATH) as f:
            owid_data = json.load(f)
        print(f"\nLoaded existing owid_data: {len(owid_data)} countries\n")
    else:
        owid_data = {}
        print("\nStarting fresh\n")

    # ── STEP 1: Bulk energy CSV ─────────────────────────────────────────────
    print("━" * 65)
    print("STEP 1: OWID bulk energy dataset (all energy + CO2 fields)")
    print("━" * 65)

    ENERGY_URL = "https://owid-public.owid.io/data/energy/owid-energy-data.csv"
    rows, fields = fetch_url(ENERGY_URL, "energy bulk")

    if rows:
        iso_col  = find_col(fields, "iso_code") or find_col(fields, "Code")
        year_col = find_col(fields, "year")     or find_col(fields, "Year")

        if iso_col and year_col:
            energy_totals = {}
            for col_name, field_name in ENERGY_FIELDS.items():
                val_col = find_col(fields, col_name)
                if not val_col:
                    print(f"  ⚠ Column not found: {col_name}")
                    continue
                extracted = extract_latest(rows, iso_col, year_col, val_col)
                n = store(owid_data, extracted, field_name)
                energy_totals[field_name] = n
                print(f"  ✓ {field_name:25s} ({col_name}) — {n} countries")
        else:
            print(f"  ✗ Could not find iso/year columns in {fields[:6]}")
    else:
        print("  ✗ Bulk energy CSV failed — energy fields will be missing")

    # ── STEP 2: Grapher chart CSVs ──────────────────────────────────────────
    print(f"\n{'━'*65}")
    print("STEP 2: Grapher chart CSVs")
    print("━" * 65 + "\n")

    success = 0
    failed  = []

    for field_name, slugs, val_kw in CHART_INDICATORS:
        print(f"[{field_name}]")
        found = False

        for slug in slugs:
            rows, fields = fetch_url(f"{BASE}/{slug}.csv")
            if not rows:
                time.sleep(0.3)
                continue

            iso_col  = find_col(fields, "Code")
            year_col = find_col(fields, "Year")
            val_col  = find_col(fields, val_kw) if val_kw else auto_val_col(fields)

            if not iso_col or not val_col:
                print(f"       ✗ Columns not found (iso={iso_col}, val={val_col})")
                time.sleep(0.3)
                continue

            extracted = extract_latest(rows, iso_col, year_col, val_col)
            if extracted:
                n = store(owid_data, extracted, field_name)
                print(f"       ✓ {field_name}: {n} countries")
                success += 1
                found = True
                break

            time.sleep(0.3)

        if not found:
            print(f"       ✗ All slugs failed")
            failed.append(field_name)

        time.sleep(0.5)

    # ── Save ────────────────────────────────────────────────────────────────
    with open(OUT_PATH, "w") as f:
        json.dump(owid_data, f, indent=2)

    total = sum(len([k for k in v if not k.endswith("_year")]) for v in owid_data.values())

    print(f"\n{'='*65}")
    print(f"✓ Saved to {OUT_PATH}")
    print(f"  Countries:           {len(owid_data)}")
    print(f"  Total field values:  {total:,}")
    print(f"  Chart indicators:    {success}/{len(CHART_INDICATORS)}")
    if failed:
        print(f"  Still failing:       {', '.join(failed)}")

    if "USA" in owid_data:
        usa = [k for k in owid_data["USA"] if not k.endswith("_year")]
        print(f"\nUSA has {len(usa)} fields:")
        for k in usa:
            print(f"  {k:30s} = {owid_data['USA'][k]}")


if __name__ == "__main__":
    main()
