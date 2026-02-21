"""
fetch_imf_data.py
Fetches key economic indicators from the IMF DataMapper API
and saves them to imf_data.json for use in the World Explorer site.

Run: python3 fetch_imf_data.py
Output: imf_data.json

IMF DataMapper API docs: https://www.imf.org/external/datamapper/api/help
"""

import requests
import json
import time

BASE = "https://www.imf.org/external/datamapper/api/v1"

# IMF indicator codes → friendly field names
# Full list available at: https://www.imf.org/external/datamapper/api/v1/indicators
INDICATORS = {
    # Growth & Output
    "NGDP_RPCH":      "gdpGrowth",          # Real GDP growth (%)
    "NGDPD":          "gdpUSD",             # GDP (current USD billions)
    "NGDPDPC":        "gdpPerCapUSD",       # GDP per capita (current USD)
    "PPPGDP":         "gdpPPP",             # GDP PPP (intl $ billions)
    "PPPPC":          "gdpPerCapPPP",       # GDP per capita PPP
    "PPPSH":          "gdpShareWorld",      # Share of world GDP PPP (%)

    # Inflation & Prices
    "PCPIPCH":        "inflation",          # Inflation, consumer prices (%)
    "PCPIEPCH":       "inflationCore",      # Core inflation (%)

    # Labour Market
    "LUR":            "unemployment",       # Unemployment rate (%)
    "LE":             "employment",         # Employment (millions)

    # Fiscal
    "GGXCNL_NGDP":   "fiscalBalance",      # Govt net lending/borrowing (% GDP)
    "GGXWDG_NGDP":   "govtDebt",           # Gross govt debt (% GDP)
    "GGR_NGDP":      "govtRevenue",        # Govt revenue (% GDP)
    "GGX_NGDP":      "govtExpenditure",    # Govt expenditure (% GDP)

    # External
    "BCA_NGDPD":      "currentAccount",    # Current account balance (% GDP)
    "TX_RPCH":        "exportGrowth",      # Export volume growth (%)
    "TM_RPCH":        "importGrowth",      # Import volume growth (%)

    # Investment & Savings
    "NID_NGDP":       "investment",        # Total investment (% GDP)
    "NGSD_NGDP":      "savings",           # Gross national savings (% GDP)
}

def fetch_indicator(imf_code, field_name):
    """Fetch latest values for all countries for one IMF indicator."""
    url = f"{BASE}/{imf_code}"
    try:
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        data = r.json()

        # Response shape: { "values": { "INDICATOR_CODE": { "COUNTRY_CODE": { "YEAR": value } } } }
        values = data.get("values", {}).get(imf_code, {})
        result = {}

        for country_code, year_data in values.items():
            if not year_data:
                continue
            # Get most recent year with a value
            latest_year = max(year_data.keys())
            latest_val = year_data[latest_year]
            if latest_val is not None:
                result[country_code] = {
                    "value": latest_val,
                    "year":  latest_year
                }

        return result

    except Exception as e:
        print(f"    ✗ Error: {e}")
        return {}

def main():
    print("=" * 55)
    print("  IMF DataMapper — World Explorer Data Fetcher")
    print("=" * 55)
    print(f"\nFetching {len(INDICATORS)} indicators...\n")

    # imf_data[country_code] = { field: value, field_year: year, ... }
    imf_data = {}

    for i, (imf_code, field_name) in enumerate(INDICATORS.items()):
        print(f"[{i+1:2d}/{len(INDICATORS)}] {field_name} ({imf_code})")
        results = fetch_indicator(imf_code, field_name)
        print(f"        → {len(results)} countries")

        for country_code, entry in results.items():
            if country_code not in imf_data:
                imf_data[country_code] = {}
            imf_data[country_code][field_name]             = entry["value"]
            imf_data[country_code][field_name + "_year"]   = entry["year"]

        time.sleep(0.5)  # be polite to IMF servers

    # Save
    out = "imf_data.json"
    with open(out, "w") as f:
        json.dump(imf_data, f, indent=2)

    print(f"\n{'='*55}")
    print(f"✓ Saved to {out}")
    print(f"  Countries with data: {len(imf_data)}")

    # Sample output
    sample = next(iter(imf_data.items()))
    print(f"\n  Sample ({sample[0]}):")
    for k, v in list(sample[1].items())[:8]:
        print(f"    {k}: {v}")

if __name__ == "__main__":
    main()
