"""
fetch_worldbank.py
Fetches country list and key indicators from the World Bank API
and saves them to ../data/worldbank/

Run from the scripts/ folder:
    python3 fetch_worldbank.py

World Bank API docs: https://datahelpdesk.worldbank.org/knowledgebase/articles/898590
"""

import requests
import json
import time
import os

BASE = "https://api.worldbank.org/v2"

# World Bank indicator codes → friendly field names
# Full list: https://data.worldbank.org/indicator
INDICATORS = {
    # Economy
    "NY.GDP.MKTP.CD":     "gdp",              # GDP (current US$)
    "NY.GDP.PCAP.CD":     "gdpPerCap",        # GDP per capita (current US$)
    "NY.GDP.PCAP.PP.CD":  "gdpPerCapPPP",     # GDP per capita, PPP (current intl $)
    "NY.GNP.PCAP.CD":     "gniPerCap",        # GNI per capita (current US$)
    "NY.GDP.MKTP.KD.ZG":  "gdpGrowth",        # GDP growth (annual %)
    "FP.CPI.TOTL.ZG":     "inflation",        # Inflation, consumer prices (%)
    "SL.UEM.TOTL.ZS":     "unemployment",     # Unemployment, total (% of labour force)
    "NE.TRD.GNFS.ZS":     "tradeGDP",         # Trade (% of GDP)
    "BX.KLT.DINV.CD.WD":  "fdi",              # FDI, net inflows (current US$)
    "SI.POV.GINI":         "gini",             # Gini index

    # Demographics
    "SP.POP.TOTL":         "population",       # Population, total
    "SP.POP.GROW":         "popGrowth",        # Population growth (%)
    "EN.POP.DNST":         "popDensity",       # Population density (people per km²)
    "SP.URB.TOTL.IN.ZS":  "urbanPct",         # Urban population (% of total)
    "SP.DYN.TFRT.IN":     "fertilityRate",    # Fertility rate (births per woman)

    # Health
    "SP.DYN.LE00.IN":     "lifeExp",          # Life expectancy at birth, total
    "SP.DYN.LE00.FE.IN":  "lifeExpFemale",    # Life expectancy, female
    "SP.DYN.LE00.MA.IN":  "lifeExpMale",      # Life expectancy, male
    "SP.DYN.IMRT.IN":     "infantMortality",  # Infant mortality rate (per 1,000)
    "SH.STA.MMRT":         "maternalMortality",# Maternal mortality ratio (per 100,000)
    "SH.XPD.CHEX.GD.ZS":  "healthSpendGDP",  # Current health expenditure (% of GDP)
    "SH.MED.PHYS.ZS":     "physicians",       # Physicians (per 1,000 people)
    "SN.ITK.DEFC.ZS":     "undernourishment", # Prevalence of undernourishment (%)

    # Education
    "SE.ADT.LITR.ZS":     "literacyRate",         # Literacy rate, adult total (%)
    "SE.PRM.NENR":         "primaryEnrollment",    # School enrollment, primary (% net)
    "SE.SEC.NENR":         "secondaryEnrollment",  # School enrollment, secondary (% net)
    "SE.TER.ENRR":         "tertiaryEnrollment",   # School enrollment, tertiary (% gross)
    "SE.XPD.TOTL.GD.ZS":  "educationSpendGDP",    # Govt expenditure on education (% of GDP)

    # Environment & Energy
    "AG.LND.FRST.ZS":     "forestArea",           # Forest area (% of land area)
    "EG.FEC.RNEW.ZS":     "renewableEnergy",       # Renewable energy consumption (%)
    "EG.ELC.ACCS.ZS":     "accessElectricity",     # Access to electricity (% of population)
    "ER.H2O.INTR.PC":     "freshwaterPerCap",      # Renewable internal freshwater (m³/capita)
    "IT.NET.USER.ZS":     "internetUsers",          # Individuals using the Internet (%)
    "IT.CEL.SETS.P2":     "mobileSubscriptions",   # Mobile subscriptions (per 100 people)

    # Poverty
    "SI.POV.DDAY":         "povertyRate",      # Poverty headcount ratio at $2.15/day (%)
    "SI.POV.LMIC":         "povertyRatio550",  # Poverty headcount ratio at $5.50/day (%)
}


def fetch_all_countries():
    """Fetch the full World Bank country list."""
    print("Fetching country list...")
    url = f"{BASE}/country?format=json&per_page=500"
    try:
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        data = r.json()
        countries = data[1]
        print(f"  → {len(countries)} countries/regions")
        return countries
    except Exception as e:
        print(f"  ✗ Error: {e}")
        return []


def fetch_indicator(wb_code):
    """Fetch latest values for all countries for one World Bank indicator."""
    url = f"{BASE}/country/all/indicator/{wb_code}?format=json&per_page=1000&mrv=1"
    try:
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        data = r.json()

        if len(data) < 2 or not data[1]:
            return {}

        result = {}
        for entry in data[1]:
            code = entry.get("countryiso3code") or entry.get("country", {}).get("id")
            val  = entry.get("value")
            year = entry.get("date")
            if code and val is not None:
                result[code] = {"value": val, "year": year}

        return result

    except Exception as e:
        print(f"    ✗ Error: {e}")
        return {}


def main():
    print("=" * 55)
    print("  World Bank — World Explorer Data Fetcher")
    print("=" * 55)

    # 1. Country list
    countries = fetch_all_countries()

    out_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "worldbank")
    out_dir = os.path.normpath(out_dir)
    os.makedirs(out_dir, exist_ok=True)

    countries_path = os.path.join(out_dir, "worldbank_data.json")
    with open(countries_path, "w") as f:
        json.dump({"countries": countries}, f, indent=2)
    print(f"✓ Saved country list to {countries_path}\n")

    # 2. Indicators
    print(f"Fetching {len(INDICATORS)} indicators...\n")
    country_data = {}

    for i, (wb_code, field_name) in enumerate(INDICATORS.items()):
        print(f"[{i+1:2d}/{len(INDICATORS)}] {field_name} ({wb_code})")
        results = fetch_indicator(wb_code)
        print(f"        → {len(results)} countries")

        for country_code, entry in results.items():
            if country_code not in country_data:
                country_data[country_code] = {}
            country_data[country_code][field_name]           = entry["value"]
            country_data[country_code][field_name + "_year"] = entry["year"]

        time.sleep(0.4)  # be polite to World Bank servers

    indicators_path = os.path.join(out_dir, "country_data.json")
    with open(indicators_path, "w") as f:
        json.dump(country_data, f, indent=2)

    print(f"\n{'='*55}")
    print(f"✓ Saved indicators to {indicators_path}")
    print(f"  Countries with data: {len(country_data)}")

    sample = next(iter(country_data.items()))
    print(f"\n  Sample ({sample[0]}):")
    for k, v in list(sample[1].items())[:8]:
        print(f"    {k}: {v}")


if __name__ == "__main__":
    main()
