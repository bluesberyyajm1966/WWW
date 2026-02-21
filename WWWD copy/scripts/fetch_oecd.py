"""
fetch_oecd.py
Fetches OECD indicators using the pandasdmx library which handles
all the SDMX complexity automatically.

First install the library:
    pip3 install pandasdmx requests

Then run from the scripts/ folder:
    python3 fetch_oecd.py
"""

import json
import time
import os

try:
    import pandasdmx as sdmx
except ImportError:
    print("Missing library. Run this first:")
    print("    pip3 install pandasdmx requests")
    exit(1)

# All 38 OECD member ISO2 codes
OECD_MEMBERS = [
    "AUS","AUT","BEL","CAN","CHL","COL","CRI","CZE","DNK","EST",
    "FIN","FRA","DEU","GRC","HUN","ISL","IRL","ISR","ITA","JPN",
    "KOR","LVA","LTU","LUX","MEX","NLD","NZL","NOR","POL","PRT",
    "SVK","SVN","ESP","SWE","CHE","TUR","GBR","USA"
]

# (dataset_id, dimension_filter, field_name, label)
# Browse datasets at: https://data.oecd.org
INDICATORS = [
    ("ANHRS",     {"LOCATION": OECD_MEMBERS},                              "hoursWorked",    "Avg Hours Worked per Year"),
    ("TUD",       {"LOCATION": OECD_MEMBERS},                              "unionDensity",   "Trade Union Density (%)"),
    ("MSTI",      {"LOCATION": OECD_MEMBERS, "SUBJECT": ["GERD_GBARD"]},   "rdSpending",     "R&D Expenditure (% GDP)"),
    ("PDB_LV",    {"LOCATION": OECD_MEMBERS, "SUBJECT": ["T_GDPHRS_V"]},   "gdpPerHour",     "GDP per Hour Worked (USD)"),
    ("SOCX_AGG",  {"LOCATION": OECD_MEMBERS, "TYPROG": ["TOT"]},           "socialSpending", "Social Spending (% GDP)"),
    ("SOCX_AGG",  {"LOCATION": OECD_MEMBERS, "TYPROG": ["OLD"]},           "pensionSpend",   "Pension Spending (% GDP)"),
    ("SOCX_AGG",  {"LOCATION": OECD_MEMBERS, "TYPROG": ["FAM"]},           "familySpend",    "Family Spending (% GDP)"),
    ("SOCX_AGG",  {"LOCATION": OECD_MEMBERS, "TYPROG": ["UNEMP"]},         "unempBenefits",  "Unemployment Benefits (% GDP)"),
    ("HEALTH_REAC",{"LOCATION": OECD_MEMBERS, "VARIABLE": ["RTOTHOSP"]},   "hospitalBeds",   "Hospital Beds per 1,000"),
    ("HEALTH_REAC",{"LOCATION": OECD_MEMBERS, "VARIABLE": ["RTOTNURSE"]},  "nurses",         "Nurses per 1,000"),
    ("HEALTH_REAC",{"LOCATION": OECD_MEMBERS, "VARIABLE": ["RTOTDOC"]},    "doctors",        "Doctors per 1,000"),
    ("PISA",      {"LOCATION": OECD_MEMBERS, "SUBJECT": ["MATH"]},         "pisaMath",       "PISA Math Score"),
    ("PISA",      {"LOCATION": OECD_MEMBERS, "SUBJECT": ["READ"]},         "pisaRead",       "PISA Reading Score"),
    ("PISA",      {"LOCATION": OECD_MEMBERS, "SUBJECT": ["SCIE"]},         "pisaScience",    "PISA Science Score"),
    ("IDD",       {"LOCATION": OECD_MEMBERS, "MEASURE": ["GINI"]},         "gini",           "Gini Coefficient"),
    ("IDD",       {"LOCATION": OECD_MEMBERS, "MEASURE": ["PVTXTOT"]},      "povertyRate",    "Poverty Rate (%)"),
    ("IDD",       {"LOCATION": OECD_MEMBERS, "MEASURE": ["MEDIAN_INC"]},   "medianIncome",   "Median Household Income"),
    ("AIR_GHG",   {"LOCATION": OECD_MEMBERS, "POLLUTANT": ["GHG"]},        "ghgEmissions",   "GHG Emissions (Mt CO2 eq)"),
    ("MUNW",      {"LOCATION": OECD_MEMBERS},                              "municipalWaste", "Municipal Waste (kg/capita)"),
    ("REVGDP",    {"LOCATION": OECD_MEMBERS, "TAX": ["TOTALTAX"]},         "taxRevenue",     "Tax Revenue (% GDP)"),
]


def fetch_indicator(oecd_conn, dataset_id, filters, field_name, label):
    """Fetch one indicator for all OECD members."""
    try:
        resp = oecd_conn.data(dataset_id, key=filters)
        df = resp.to_pandas(dtype=float).reset_index()

        # Find country and value columns
        loc_col  = next((c for c in df.columns if c in ("LOCATION", "REF_AREA")), None)
        time_col = next((c for c in df.columns if c in ("TIME_PERIOD", "TIME", "Year")), None)
        val_col  = next((c for c in df.columns if c in ("value", "OBS_VALUE", "Value")), None)

        if loc_col is None or val_col is None:
            print(f"    ✗ Unexpected columns: {list(df.columns)}")
            return {}

        # Keep most recent non-null value per country
        df = df.dropna(subset=[val_col])
        if time_col:
            df = df.sort_values(time_col, ascending=False)
        df = df.drop_duplicates(subset=[loc_col], keep="first")

        result = {}
        for _, row in df.iterrows():
            country = str(row[loc_col])
            val     = float(row[val_col])
            year    = str(row[time_col]) if time_col else ""
            result[country] = {"value": val, "year": year}

        return result

    except Exception as e:
        print(f"    ✗ Error: {e}")
        return {}


def main():
    print("=" * 55)
    print("  OECD — World Explorer Data Fetcher")
    print("=" * 55)
    print(f"\nFetching {len(INDICATORS)} indicators...\n")

    oecd = sdmx.Request("OECD")
    oecd_data = {}

    for i, (dataset_id, filters, field_name, label) in enumerate(INDICATORS):
        print(f"[{i+1:2d}/{len(INDICATORS)}] {label}")
        results = fetch_indicator(oecd, dataset_id, filters, field_name, label)
        print(f"        → {len(results)} countries")

        for country_code, entry in results.items():
            if country_code not in oecd_data:
                oecd_data[country_code] = {}
            oecd_data[country_code][field_name]           = entry["value"]
            oecd_data[country_code][field_name + "_year"] = entry["year"]

        time.sleep(0.3)

    out_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "oecd", "oecd_data.json")
    out_path = os.path.normpath(out_path)
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    with open(out_path, "w") as f:
        json.dump(oecd_data, f, indent=2)

    print(f"\n{'='*55}")
    print(f"✓ Saved to {out_path}")
    print(f"  Countries with data: {len(oecd_data)}")

    if oecd_data:
        sample = next(iter(oecd_data.items()))
        print(f"\n  Sample ({sample[0]}):")
        for k, v in list(sample[1].items())[:8]:
            print(f"    {k}: {v}")


if __name__ == "__main__":
    main()
