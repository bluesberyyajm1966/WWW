"""
fetch_sipri.py
Fetches military expenditure data from the SIPRI Military Expenditure Database.

Source: https://www.sipri.org/databases/milex
Data covers 1949-2024, updated annually.

The Excel file has multiple sheets:
  - Current USD     : military spending in current US dollars
  - Constant (2023) USD : spending in inflation-adjusted dollars
  - Share of GDP    : military expenditure as % of GDP
  - Per capita      : spending per person (current USD)
  - Share of Govt   : spending as % of government expenditure

We extract the most recent year's value from each sheet per country.

Run from your WWWD folder:
    pip3 install requests pandas openpyxl
    python3 fetch_sipri.py

Output: data/sipri/sipri_data.json
"""

import requests
import json
import os
import io
import pandas as pd

OUT_PATH  = os.path.join("data", "sipri", "sipri_data.json")
CACHE_DIR = os.path.join("data", "sipri", "cache")
CACHE_FILE = os.path.join(CACHE_DIR, "SIPRI-Milex.xlsx")
HEADERS   = {"User-Agent": "Mozilla/5.0 WorldExplorer/1.0"}

# Confirmed direct download URL (1949-2024 edition)
XLSX_URL = "https://www.sipri.org/sites/default/files/SIPRI-Milex-data-1949-2024_2.xlsx"

# Sheet name → (our field name, description)
SHEETS = {
    "Current US$":               ("milexCurrentUSD",  "Military spend, current USD (millions)",    1),
    "Constant (2023) US$":       ("milexConstantUSD", "Military spend, constant 2023 USD (millions)", 1),
    "Share of GDP":              ("milexPctGDP",       "Military spend as % of GDP",               100),
    "Per capita":                ("milexPerCapita",    "Military spend per capita, current USD",     1),
    "Share of Govt. spending":   ("milexPctGovt",      "Military spend as % of govt expenditure",  100),
}

# Country name → ISO3 overrides for names that don't match pycountry
# Add more here if you see mismatches in the output
NAME_TO_ISO3 = {
    "United States":                    "USA",
    "United Kingdom":                   "GBR",
    "Russia":                           "RUS",
    "South Korea":                      "KOR",
    "North Korea":                      "PRK",
    "Iran":                             "IRN",
    "Syria":                            "SYR",
    "Venezuela":                        "VEN",
    "Bolivia":                          "BOL",
    "Tanzania":                         "TZA",
    "Congo, Dem. Rep.":                 "COD",
    "Congo, Rep.":                      "COG",
    "Congo, DR":                        "COD",
    "Congo, Republic":                  "COG",
    "Côte d'Ivoire":                    "CIV",
    "Gambia, The":                      "GMB",
    "Korea, South":                     "KOR",
    "Korea, North":                     "PRK",
    "Lao PDR":                          "LAO",
    "Laos":                             "LAO",
    "Libya":                            "LBY",
    "Moldova":                          "MDA",
    "Slovakia":                         "SVK",
    "Czechia":                          "CZE",
    "Czech Republic":                   "CZE",
    "Kyrgyzstan":                       "KGZ",
    "Kyrgyz Republic":                  "KGZ",
    "Timor-Leste":                      "TLS",
    "Eswatini":                         "SWZ",
    "Swaziland":                        "SWZ",
    "Macedonia":                        "MKD",
    "North Macedonia":                  "MKD",
    "Micronesia":                       "FSM",
    "São Tomé and Príncipe":            "STP",
    "Brunei":                           "BRN",
    "Cape Verde":                       "CPV",
    "Cabo Verde":                       "CPV",
    "Palestine":                        "PSE",
    "West Bank and Gaza":               "PSE",
    "Taiwan":                           "TWN",
    "Kosovo":                           "XKX",
    "Vietnam":                          "VNM",
    "Viet Nam":                         "VNM",
    "Myanmar":                          "MMR",
    "Burma":                            "MMR",
    "Egypt":                            "EGY",
    "Turkey":                           "TUR",
    "Türkiye":                          "TUR",
}


def get_xlsx():
    """Download the Excel file, using cache if available."""
    os.makedirs(CACHE_DIR, exist_ok=True)

    if os.path.exists(CACHE_FILE):
        print(f"  Using cached file: {CACHE_FILE}")
        with open(CACHE_FILE, "rb") as f:
            return f.read()

    print(f"  Downloading from: {XLSX_URL}")
    try:
        r = requests.get(XLSX_URL, timeout=120, headers=HEADERS, stream=True)
        r.raise_for_status()
        chunks = []
        downloaded = 0
        total = int(r.headers.get("Content-Length", 0))
        for chunk in r.iter_content(chunk_size=512 * 1024):
            chunks.append(chunk)
            downloaded += len(chunk)
            if total:
                print(f"  {downloaded/1e6:.1f} MB / {total/1e6:.1f} MB ({downloaded/total*100:.0f}%)", end="\r")
        data = b"".join(chunks)
        print(f"  ✓ Downloaded {downloaded/1e6:.1f} MB        ")

        with open(CACHE_FILE, "wb") as f:
            f.write(data)
        print(f"  Cached to {CACHE_FILE}")
        return data

    except Exception as e:
        print(f"  ✗ Download failed: {e}")
        print(f"  Try manually downloading from:")
        print(f"    https://www.sipri.org/databases/milex")
        print(f"  and saving as: {CACHE_FILE}")
        return None


def country_to_iso3(name):
    """Map a country name string to ISO3. Uses manual map first, then pycountry."""
    name = str(name).strip()

    # Manual overrides
    if name in NAME_TO_ISO3:
        return NAME_TO_ISO3[name]

    # Try pycountry if available
    try:
        import pycountry
        # Try exact match
        c = pycountry.countries.get(name=name)
        if c:
            return c.alpha_3
        # Try fuzzy search
        results = pycountry.countries.search_fuzzy(name)
        if results:
            return results[0].alpha_3
    except Exception:
        pass

    return None


def parse_sheet(xl, sheet_name, field_name, description, multiply_by=1):
    """
    Parse a SIPRI milex sheet and return {iso3: {field: value, field_year: year}}.

    SIPRI sheet structure:
      - Row 1-5: header/notes (skip)
      - Row 6: column headers — "Country" then year columns (1949, 1950, ...)
      - Rows 7+: country data
      - Values may be "xxx" (not applicable), "..." (not available), or numeric
    """
    print(f"\n  Sheet: '{sheet_name}' → {field_name}")
    try:
        df = pd.read_excel(xl, sheet_name=sheet_name, header=None)
    except Exception as e:
        print(f"  ✗ Could not read sheet: {e}")
        return {}

    # Find the header row (contains "Country" and year numbers)
    header_row = None
    for i, row in df.iterrows():
        vals = [str(v).strip() for v in row if str(v).strip()]
        if any(v == "Country" for v in vals) or any(v.isdigit() and len(v) == 4 for v in vals):
            header_row = i
            break

    if header_row is None:
        print(f"  ✗ Could not find header row")
        return {}

    # Set headers and slice data
    df.columns = df.iloc[header_row]
    df = df.iloc[header_row + 1:].reset_index(drop=True)

    # Find the country column
    country_col = next((c for c in df.columns if str(c).strip().lower() == "country"), None)
    if not country_col:
        country_col = df.columns[0]

    # Find year columns (4-digit numbers)
    year_cols = [c for c in df.columns if str(c).strip().isdigit() and len(str(c).strip()) == 4]
    if not year_cols:
        print(f"  ✗ No year columns found. Columns: {list(df.columns)[:10]}")
        return {}

    year_cols_sorted = sorted(year_cols, key=lambda x: int(str(x).strip()), reverse=True)
    print(f"  Years available: {str(int(str(year_cols_sorted[-1]))).strip()}–{str(int(str(year_cols_sorted[0]))).strip()}")

    result = {}
    skipped = []

    for _, row in df.iterrows():
        name = str(row[country_col]).strip()
        if not name or name.lower() in ("country", "nan", "notes", "region", "world"):
            continue
        # Skip region subtotals (all caps or starts with spaces)
        if name.isupper() and len(name) > 3:
            continue

        iso3 = country_to_iso3(name)
        if not iso3:
            skipped.append(name)
            continue

        # Find most recent non-null, non-placeholder value
        value = None
        year = None
        for yc in year_cols_sorted:
            raw = str(row.get(yc, "")).strip()
            if raw in ("xxx", "...", "", "nan", "NaN"):
                continue
            try:
                value = float(raw.replace(",", "")) * multiply_by
                year = str(int(str(yc).strip()))
                break
            except ValueError:
                continue

        if value is None:
            continue

        result.setdefault(iso3, {})
        result[iso3][field_name]           = value
        result[iso3][field_name + "_year"] = year

    if skipped:
        print(f"  ⚠ Could not match to ISO3 ({len(skipped)}): {skipped[:10]}" +
              (" ..." if len(skipped) > 10 else ""))
    print(f"  ✓ {len(result)} countries")
    return result


def main():
    print("=" * 65)
    print("  SIPRI Military Expenditure Fetcher")
    print("  Source: SIPRI Military Expenditure Database 1949–2024")
    print("=" * 65)
    print(f"  Cache: {os.path.abspath(CACHE_DIR)}")
    print(f"  (Delete cache to force re-download)\n")

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)

    # Load existing data
    if os.path.exists(OUT_PATH):
        with open(OUT_PATH) as f:
            sipri_data = json.load(f)
        print(f"Loaded existing: {len(sipri_data)} countries")
    else:
        sipri_data = {}

    # Download / load from cache
    xlsx_bytes = get_xlsx()
    if not xlsx_bytes:
        return

    xl = pd.ExcelFile(io.BytesIO(xlsx_bytes))
    print(f"\n  Sheets in file: {xl.sheet_names}")

    # Parse each sheet
    for sheet_name, (field_name, description, multiply_by) in SHEETS.items():
        actual = next((s for s in xl.sheet_names if sheet_name.lower() in s.lower()), None)
        if not actual:
            print(f"\n  ⚠ Sheet '{sheet_name}' not found — skipping")
            continue

        results = parse_sheet(xl, actual, field_name, description, multiply_by)
        for iso3, fields in results.items():
            sipri_data.setdefault(iso3, {}).update(fields)

    # Save
    with open(OUT_PATH, "w") as f:
        json.dump(sipri_data, f, indent=2)

    total = sum(len([k for k in v if not k.endswith("_year")]) for v in sipri_data.values())
    print(f"\n{'='*65}")
    print(f"✓ Saved to {OUT_PATH}")
    print(f"  Countries:          {len(sipri_data)}")
    print(f"  Total field values: {total:,}")

    # Sample output for a few major countries
    for iso3 in ["USA", "CHN", "RUS", "GBR", "DEU"]:
        if iso3 in sipri_data:
            d = sipri_data[iso3]
            gdp = d.get("milexPctGDP")
            yr  = d.get("milexPctGDP_year", "?")
            usd = d.get("milexCurrentUSD")
            cap = d.get("milexPerCapita")
            gdp_s = f"{gdp:.2f}% GDP ({yr})" if gdp is not None else "—% GDP"
            usd_s = f"${usd:,.0f}M total"     if usd is not None else "—M total"
            cap_s = f"${cap:.0f}/capita"       if cap is not None else "—/capita"
            print(f"  {iso3}: {gdp_s}, {usd_s}, {cap_s}")


if __name__ == "__main__":
    main()
