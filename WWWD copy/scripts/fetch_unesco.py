"""
fetch_unesco.py
Fetches education & science data from UNESCO Institute for Statistics (UIS).

Uses the UIS Bulk Data Download Service (BDDS).
Files are distributed as ZIP archives containing CSVs.

Run from your WWWD folder:
    pip3 install requests pandas
    python3 fetch_unesco.py

Output: data/unesco/unesco_data.json

CACHING: ZIPs are saved to data/unesco/cache/ so re-runs skip the download.
         Delete that folder to force a fresh download.
"""

import requests
import json
import os
import io
import time
import zipfile
import pandas as pd

OUT_PATH   = os.path.join("data", "unesco", "unesco_data.json")
CACHE_DIR  = os.path.join("data", "unesco", "cache")
HEADERS    = {"User-Agent": "Mozilla/5.0 WorldExplorer/1.0"}

UNESCO_API_KEY = ""

# ── URLS ───────────────────────────────────────────────────────────────────
# Confirmed working: https://download.uis.unesco.org/bdds/202509/SDG.zip
# Slug format: YYYYMM. We try newest first.

RELEASE_SLUGS = [
    "202509",   # September 2025 confirmed working
    "202504",
    "202409",
    "202404",
]

BULK_BASE_PATTERN = "https://download.uis.unesco.org/bdds/{slug}/{zip_name}.zip"

# ── INDICATOR MAPS ─────────────────────────────────────────────────────────

# Confirmed present in SDG_DATA_NATIONAL.csv
SDG_INDICATORS = {
    "LR.AG15T99":        "literacyAdult",
    "LR.AG15T24":        "literacyYouth",
    "LR.AG15T99.F":      "literacyAdultF",
    "CR.1":              "primaryCompletion",
    "CR.2":              "lowerSecCompletion",
    "CR.3":              "upperSecCompletion",
    "XGDP.FSGOV.FFNTR":  "govtEduSpendGDP",

    # These were not found in SDG — may be in OPRI or use different codes.
    # After running once, check the "Sample of actual OPRI codes" output and
    # move confirmed codes here or into OPRI_INDICATORS below.
    # "NERA.1.CP":         "primaryNetEnrol",
    # "ROFST.1":           "outOfSchoolPrimary",
    # "PTRHC.1":           "pupilTeacherPrimary",
    # "TRTA.1.INST1":      "trainedTeachersPrimary",
    # "NERA.2":            "lowerSecNetEnrol",
    # "NERA.3":            "upperSecNetEnrol",
    # "ROFST.2":           "outOfSchoolLowerSec",
    # "PTRHC.2":           "pupilTeacherSecondary",
    # "GERT.5T8":          "tertiaryGrossEnrol",
    # "GERT.5T8.F":        "tertiaryEnrolF",
    # "XUNIT.FSGOV.FFNTR.L1.GDPCAP.PPP": "spendPerPupilPrimary",
    # "XGOVEXP.FSGOV":     "govtEduSpendBudget",
    # "GPI.NERA.1":        "gpiPrimary",
    # "GPI.NERA.2":        "gpiLowerSec",
    # "GPI.GERT.5T8":      "gpiTertiary",
    # "PRPFP.L1.MATH":     "profMathPrimary",
    # "PRPFP.L1.READ":     "profReadPrimary",
    # "PRPFP.L2.MATH":     "profMathLowerSec",
    # "PRPFP.L2.READ":     "profReadLowerSec",
    # "TRTA.2.INST1":      "trainedTeachersSec",
    # "SCHBSP.1.ELEC":     "schoolsWithElec",
    # "SCHBSP.1.INT":      "schoolsWithInternet",
    # "SCHBSP.2.INT":      "schoolsWithInternetSec",
}

# OPRI indicators — UPDATE THESE after seeing "Sample of actual OPRI codes" below
OPRI_INDICATORS = {
    "GERD.HERD.GDP":     "rdSpendGDP",
    "GERD.GOVERD.GDP":   "govRdGDP",
    "FTE.TOTAL.SC":      "researchersPerMillion",
    "FTE.TOTAL.F.SC":    "researchersFemale",
    "PAT.RESD.P6":       "patentApplications",
}


# ── HELPERS ────────────────────────────────────────────────────────────────

def get_zip(zip_name):
    """
    Return ZIP bytes from cache if available, otherwise download.
    Saves to CACHE_DIR so re-runs are instant.
    """
    os.makedirs(CACHE_DIR, exist_ok=True)
    cache_path = os.path.join(CACHE_DIR, f"{zip_name}.zip")

    if os.path.exists(cache_path):
        print(f"  Using cached: {cache_path}")
        with open(cache_path, "rb") as f:
            return f.read()

    for slug in RELEASE_SLUGS:
        url = BULK_BASE_PATTERN.format(slug=slug, zip_name=zip_name)
        print(f"  Trying: {url}")
        try:
            r = requests.get(url, timeout=300, headers=HEADERS, stream=True)
            if r.status_code == 404:
                print(f"  → 404, trying next slug...")
                continue
            r.raise_for_status()

            chunks = []
            downloaded = 0
            total = int(r.headers.get("Content-Length", 0))
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                chunks.append(chunk)
                downloaded += len(chunk)
                if total:
                    print(f"  {downloaded/1e6:.1f} MB / {total/1e6:.1f} MB ({downloaded/total*100:.0f}%)", end="\r")
            data = b"".join(chunks)
            print(f"  ✓ Downloaded {downloaded/1e6:.1f} MB        ")

            # Verify it's actually a ZIP (PK magic bytes), not an HTML redirect
            if not data[:2] == b"PK":
                print(f"  ✗ Not a ZIP file (got HTML redirect). Trying next slug...")
                continue

            with open(cache_path, "wb") as f:
                f.write(data)
            print(f"  Cached to {cache_path}")
            return data

        except Exception as e:
            print(f"  → Error: {e}")
            continue

    print(f"\n  ✗ All slugs failed for {zip_name}.zip")
    print(f"  Go to: https://databrowser.uis.unesco.org/documentation/bulk")
    print(f"  Right-click the download button → Copy Link Address")
    print(f"  Extract the YYYYMM slug and add it to RELEASE_SLUGS at the top of this script")
    return None


def extract_csv(zip_bytes, csv_name):
    """Extract a named CSV from ZIP bytes. Returns string content or None."""
    try:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            names = zf.namelist()
            print(f"  ZIP contents: {names}")
            match = next(
                (n for n in names if csv_name.upper() in n.upper() and n.upper().endswith(".CSV")),
                None
            )
            if not match:
                print(f"  ✗ {csv_name}.csv not found in ZIP")
                return None
            print(f"  Extracting: {match}")
            with zf.open(match) as f:
                return f.read().decode("utf-8", errors="replace")
    except Exception as e:
        print(f"  ✗ ZIP extraction failed: {e}")
        return None


def print_opri_labels(zip_bytes):
    """
    Read OPRI_LABEL.csv from the ZIP and print numeric code → indicator name,
    filtered to science/R&D/innovation related indicators.
    """
    try:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            match = next((n for n in zf.namelist() if "LABEL" in n.upper() and n.upper().endswith(".CSV")), None)
            if not match:
                print("  ✗ No LABEL csv found in OPRI ZIP")
                return
            with zf.open(match) as f:
                df = pd.read_csv(f, low_memory=False)
            df.columns = [c.strip().upper() for c in df.columns]
            print(f"  Label file columns: {list(df.columns)}")

            # Find the indicator ID and name columns
            id_col   = next((c for c in df.columns if "INDICATOR" in c and "ID" in c), df.columns[0])
            name_col = next((c for c in df.columns if "INDICATOR" in c and ("NAME" in c or "LABEL" in c or "EN" in c)), df.columns[1] if len(df.columns) > 1 else None)

            if not name_col:
                print(f"  Available cols: {list(df.columns)}")
                return

            # Filter to science/R&D/innovation keywords
            keywords = ["research", "r&d", "gerd", "patent", "researcher",
                        "innovation", "science", "technology", "fte", "expenditure"]
            mask = df[name_col].str.lower().str.contains("|".join(keywords), na=False)
            relevant = df[mask][[id_col, name_col]].drop_duplicates()

            print(f"\n  ── OPRI Science/R&D indicators (numeric ID → name) ──")
            for _, row in relevant.iterrows():
                print(f"    {str(row[id_col]):10s}  {row[name_col]}")
            print(f"  ── Use these IDs in OPRI_INDICATORS ──\n")
    except Exception as e:
        print(f"  ✗ Label lookup failed: {e}")


def process_csv(content, indicator_map, label, show_sample_codes=False):
    """
    Parse UIS bulk CSV, return {iso3: {field: value, field_year: year}}.
    If show_sample_codes=True, prints actual indicator codes from the file.
    """
    print(f"\n  Parsing {label}...")
    try:
        df = pd.read_csv(io.StringIO(content),
                         dtype={"YEAR": str, "VALUE": str},
                         low_memory=False)
        print(f"  Rows: {len(df):,}  |  Cols: {list(df.columns)}")
    except Exception as e:
        print(f"  Parse error: {e}")
        return {}

    df.columns = [c.strip().upper() for c in df.columns]

    country_col   = next((c for c in df.columns if "COUNTRY" in c and "ID" in c), None)
    year_col      = next((c for c in df.columns if c == "YEAR"), None)
    indicator_col = next((c for c in df.columns if "INDICATOR" in c and "ID" in c), None)
    value_col     = next((c for c in df.columns if c == "VALUE"), None)

    if not all([country_col, year_col, indicator_col, value_col]):
        print(f"  ✗ Missing columns: country={country_col}, year={year_col}, "
              f"indicator={indicator_col}, value={value_col}")
        return {}

    # Diagnostic
    all_codes = set(str(x) for x in df[indicator_col].dropna().unique())
    our_codes = list(indicator_map.keys())
    matched = [c for c in our_codes if c in all_codes]
    missed  = [c for c in our_codes if c not in all_codes]
    print(f"  Matched {len(matched)}/{len(our_codes)} indicator codes: {matched}")
    if missed:
        print(f"  Not found: {missed}")

    if show_sample_codes or not matched:
        sample = sorted(all_codes, key=lambda x: str(x))[:60]
        print(f"\n  ── Sample of actual indicator codes in {label} ──")
        for code in sample:
            print(f"    {code}")
        print(f"  ── Update OPRI_INDICATORS in the script using the codes above ──\n")

    if not matched:
        return {}

    df = df[df[indicator_col].isin(our_codes)].copy()
    df[value_col] = pd.to_numeric(df[value_col], errors="coerce")
    df = df.dropna(subset=[value_col])
    df[year_col] = pd.to_numeric(df[year_col], errors="coerce")
    df = df.dropna(subset=[year_col])
    df = df.sort_values(year_col, ascending=False)
    df = df.drop_duplicates(subset=[country_col, indicator_col], keep="first")

    result = {}
    for _, row in df.iterrows():
        iso3 = str(row[country_col]).strip().upper()
        if not iso3 or len(iso3) != 3:
            continue
        uis_code   = str(row[indicator_col]).strip()
        field_name = indicator_map.get(uis_code)
        if not field_name:
            continue
        result.setdefault(iso3, {})
        result[iso3][field_name]           = float(row[value_col])
        result[iso3][field_name + "_year"] = str(int(row[year_col]))

    total_fields = sum(len([k for k in v if not k.endswith("_year")]) for v in result.values())
    print(f"  ✓ {len(result)} countries, {total_fields:,} field values")
    return result


# ── MAIN ───────────────────────────────────────────────────────────────────

def main():
    print("=" * 65)
    print("  UNESCO UIS — World Explorer Data Fetcher")
    print("=" * 65)
    print(f"  Cache: {os.path.abspath(CACHE_DIR)}")
    print(f"  (Delete cache folder to force fresh downloads)\n")

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)

    if os.path.exists(OUT_PATH):
        with open(OUT_PATH) as f:
            unesco_data = json.load(f)
        print(f"Loaded existing: {len(unesco_data)} countries")
    else:
        unesco_data = {}

    steps = [
        ("SDG",  "SDG_DATA_NATIONAL",  "SDG 4 Education",     SDG_INDICATORS,  False),
        ("OPRI", "OPRI_DATA_NATIONAL", "OPRI Science/Policy", OPRI_INDICATORS, True),
    ]

    for step_num, (zip_name, csv_name, label, indicator_map, show_sample) in enumerate(steps, 1):
        print(f"\n{'━'*65}")
        print(f"STEP {step_num}: {label}  ({zip_name}.zip → {csv_name}.csv)")
        print("━" * 65)

        zip_bytes = get_zip(zip_name)
        if not zip_bytes:
            continue

        content = extract_csv(zip_bytes, csv_name)

        # For OPRI: print label lookup so we can identify the right numeric codes
        if zip_name == "OPRI":
            print("\n  Looking up OPRI indicator labels...")
            print_opri_labels(zip_bytes)

        del zip_bytes

        if not content:
            continue

        results = process_csv(content, indicator_map, label, show_sample_codes=show_sample)
        del content

        for iso3, fields in results.items():
            if zip_name == "OPRI":
                for k, v in fields.items():
                    if k not in unesco_data.get(iso3, {}):
                        unesco_data.setdefault(iso3, {})[k] = v
            else:
                unesco_data.setdefault(iso3, {}).update(fields)

    # ── Optional REST API ──────────────────────────────────────────────────
    if UNESCO_API_KEY:
        print(f"\n{'━'*65}")
        print("STEP 3: REST API — PISA scores")
        print("━" * 65)
        for code, field in [
            ("LO.PISA.MAT", "pisaMath"),
            ("LO.PISA.REA", "pisaRead"),
            ("LO.PISA.SCI", "pisaScience"),
        ]:
            url = f"https://api.uis.unesco.org/api/public/v1/data/indicators/{code}"
            try:
                r = requests.get(url, params={"apiKey": UNESCO_API_KEY, "format": "json"},
                                 timeout=30, headers=HEADERS)
                r.raise_for_status()
                count = 0
                for entry in r.json().get("data", []):
                    iso3 = entry.get("geoUnit", {}).get("id", "")
                    if not iso3 or len(iso3) != 3:
                        continue
                    best = max(entry.get("observations", []),
                               key=lambda o: o.get("year", 0), default=None)
                    if best and best.get("value") is not None and field not in unesco_data.get(iso3, {}):
                        unesco_data.setdefault(iso3, {})[field]           = float(best["value"])
                        unesco_data.setdefault(iso3, {})[field + "_year"] = str(best["year"])
                        count += 1
                print(f"  ✓ {field}: {count} countries")
            except Exception as e:
                print(f"  ✗ {field}: {e}")
            time.sleep(0.3)

    # ── Save ───────────────────────────────────────────────────────────────
    with open(OUT_PATH, "w") as f:
        json.dump(unesco_data, f, indent=2)

    total = sum(len([k for k in v if not k.endswith("_year")]) for v in unesco_data.values())
    print(f"\n{'='*65}")
    print(f"✓ Saved to {OUT_PATH}")
    print(f"  Countries:          {len(unesco_data)}")
    print(f"  Total field values: {total:,}")

    if "USA" in unesco_data:
        usa_fields = [k for k in unesco_data["USA"] if not k.endswith("_year")]
        print(f"\nUSA fields ({len(usa_fields)}):")
        for k in sorted(usa_fields):
            yr = unesco_data["USA"].get(k + "_year", "?")
            print(f"  {k:40s} = {unesco_data['USA'][k]:.2f}  ({yr})")


if __name__ == "__main__":
    main()
