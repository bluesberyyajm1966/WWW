"""
fetch_imf_data.py
Fetches key economic indicators from the IMF DataMapper API
and saves them to ../data/imf/imf_data.json

Run from the scripts/ folder:
    python3 fetch_imf_data.py

IMF DataMapper API docs: https://www.imf.org/external/datamapper/api/help
"""

import requests
import json
import time
import os

BASE = "https://www.imf.org/external/datamapper/api/v1"

# IMF indicator codes → friendly field names
# Full list: https://www.imf.org/external/datamapper/api/v1/indicators
INDICATORS = {
    # Growth & Output
    "NGDP_RPCH":    "gdpGrowth",       # Real GDP growth (%)
    "NGDPD":        "gdpUSD",          # GDP (current USD billions)
    "NGDPDPC":      "gdpPerCapUSD",    # GDP per capita (current USD)
    "PPPGDP":       "gdpPPP",          # GDP PPP (intl $ billions)
    "PPPPC":        "gdpPerCapPPP",    # GDP per capita PPP
    "PPPSH":        "gdpShareWorld",   # Share of world GDP PPP (%)

    # Inflation & Prices
    "PCPIPCH":      "inflation",       # Inflation, consumer prices (%)
    "PCPIEPCH":     "inflationCore",   # Core inflation (%)

    # Labour Market
    "LUR":          "unemployment",    # Unemployment rate (%)
    "LE":           "employment",      # Employment (millions)

    # Fiscal
    "GGXCNL_NGDP":  "fiscalBalance",   # Govt net lending/borrowing (% GDP)
    "GGXWDG_NGDP":  "govtDebt",        # Gross govt debt (% GDP)
    "GGR_NGDP":     "govtRevenue",     # Govt revenue (% GDP)
    "GGX_NGDP":     "govtExpenditure", # Govt expenditure (% GDP)

    # External
    "BCA_NGDPD":    "currentAccount",  # Current account balance (% GDP)
    "TX_RPCH":      "exportGrowth",    # Export volume growth (%)
    "TM_RPCH":      "importGrowth",    # Import volume growth (%)

    # Investment & Savings
    "NID_NGDP":     "investment",      # Total investment (% GDP)
    "NGSD_NGDP":    "savings",         # Gross national savings (% GDP)
}


def fetch_indicator(imf_code):
    """Fetch latest values for all countries for one IMF indicator."""
    url = f"{BASE}/{imf_code}"
    try:
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        data = r.json()

        # Response: { "values": { "INDICATOR": { "COUNTRY": { "YEAR": value } } } }
        values = data.get("values", {}).get(imf_code, {})
        result = {}

        for country_code, year_data in values.items():
            if not year_data:
                continue
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

    imf_data = {}

    for i, (imf_code, field_name) in enumerate(INDICATORS.items()):
        print(f"[{i+1:2d}/{len(INDICATORS)}] {field_name} ({imf_code})")
        results = fetch_indicator(imf_code)
        print(f"        → {len(results)} countries")

        for country_code, entry in results.items():
            if country_code not in imf_data:
                imf_data[country_code] = {}
            imf_data[country_code][field_name]           = entry["value"]
            imf_data[country_code][field_name + "_year"] = entry["year"]

        time.sleep(0.5)  # be polite to IMF servers

    # Save to ../data/imf/ relative to this script's location
    out_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "imf", "imf_data.json")
    out_path = os.path.normpath(out_path)
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    with open(out_path, "w") as f:
        json.dump(imf_data, f, indent=2)

    print(f"\n{'='*55}")
    print(f"✓ Saved to {out_path}")
    print(f"  Countries with data: {len(imf_data)}")

    sample = next(iter(imf_data.items()))
    print(f"\n  Sample ({sample[0]}):")
    for k, v in list(sample[1].items())[:8]:
        print(f"    {k}: {v}")


if __name__ == "__main__":
    main()
