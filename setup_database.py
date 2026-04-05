"""
setup_database.py
-----------------
Downloads a curated subset of World Bank World Development Indicators and
populates a local SQLite database.

Run once before launching the app:
    python setup_database.py

Data source: World Bank Open Data (CC BY 4.0)
https://data.worldbank.org/
"""

import sqlite3
import requests
import pandas as pd
import time
import sys
import os
from pathlib import Path

DB_PATH = Path(__file__).parent / "world_bank.db"

# ---------------------------------------------------------------------------
# Indicators: chosen to cover the major thematic areas of the WDI and to
# support a wide range of plausible natural-language questions.
# ---------------------------------------------------------------------------
INDICATORS = {
    # --- Economic ---
    "NY.GDP.PCAP.CD":  ("GDP per capita (current US$)",                   "Economic",      "current US$"),
    "NY.GDP.MKTP.CD":  ("GDP (current US$)",                              "Economic",      "current US$"),
    "NY.GNP.PCAP.CD":  ("GNI per capita, Atlas method (current US$)",     "Economic",      "current US$"),
    "FP.CPI.TOTL.ZG":  ("Inflation, consumer prices (annual %)",          "Economic",      "percent"),
    "NE.EXP.GNFS.ZS":  ("Exports of goods and services (% of GDP)",       "Economic",      "percent"),
    "GC.DOD.TOTL.GD.ZS": ("Central government debt, total (% of GDP)",    "Economic",      "percent"),
    # --- Demographics ---
    "SP.POP.TOTL":     ("Population, total",                              "Demographics",  "persons"),
    "SP.POP.GROW":     ("Population growth (annual %)",                   "Demographics",  "percent"),
    "SP.URB.TOTL.IN.ZS": ("Urban population (% of total)",               "Demographics",  "percent"),
    "SP.DYN.TFRT.IN":  ("Fertility rate, total (births per woman)",       "Demographics",  "births per woman"),
    # --- Health ---
    "SP.DYN.LE00.IN":  ("Life expectancy at birth, total (years)",        "Health",        "years"),
    "SP.DYN.IMRT.IN":  ("Mortality rate, infant (per 1,000 live births)", "Health",        "per 1,000 live births"),
    "SH.XPD.CHEX.GD.ZS": ("Current health expenditure (% of GDP)",       "Health",        "percent"),
    "SH.STA.MALN.ZS":  ("Prevalence of undernourishment (% of population)", "Health",     "percent"),
    # --- Education ---
    "SE.ADT.LITR.ZS":  ("Literacy rate, adult total (% 15+ years)",       "Education",     "percent"),
    "SE.XPD.TOTL.GD.ZS": ("Government expenditure on education (% of GDP)", "Education", "percent"),
    "SE.PRM.NENR":     ("School enrollment, primary (% net)",             "Education",     "percent"),
    # --- Environment ---
    "EN.ATM.CO2E.PC":  ("CO2 emissions (metric tons per capita)",         "Environment",   "metric tons per capita"),
    "EG.USE.PCAP.KG.OE": ("Energy use (kg of oil equivalent per capita)", "Environment",  "kg of oil equivalent per capita"),
    "AG.LND.FRST.ZS":  ("Forest area (% of land area)",                  "Environment",   "percent"),
    # --- Technology & Connectivity ---
    "IT.NET.USER.ZS":  ("Individuals using the Internet (% of population)", "Technology", "percent"),
    "IT.CEL.SETS.P2":  ("Mobile cellular subscriptions (per 100 people)", "Technology",   "per 100 people"),
    # --- Poverty & Inequality ---
    "SI.POV.DDAY":     ("Poverty headcount ratio at $2.15/day (2017 PPP, % of population)", "Poverty", "percent"),
    "SI.POV.GINI":     ("Gini index (World Bank estimate)",               "Poverty",       "index (0-100)"),
    # --- Labor ---
    "SL.UEM.TOTL.ZS":  ("Unemployment, total (% of total labor force)",   "Labor",         "percent"),
    "SL.TLF.CACT.FE.ZS": ("Labor force participation rate, female (% of female population 15+)", "Labor", "percent"),
}

# Years to fetch
YEAR_START = 2000
YEAR_END   = 2023

# World Bank API endpoint
WB_API = "https://api.worldbank.org/v2"


def fetch_wb_indicator(indicator_code: str, year_start: int, year_end: int) -> pd.DataFrame:
    """Fetch all country-year values for one World Bank indicator."""
    rows = []
    page = 1
    per_page = 1000

    while True:
        url = (
            f"{WB_API}/country/all/indicator/{indicator_code}"
            f"?format=json&per_page={per_page}&page={page}"
            f"&date={year_start}:{year_end}"
        )
        try:
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as exc:
            print(f"  Warning: failed to fetch {indicator_code} page {page}: {exc}")
            break

        data = resp.json()
        if not data or len(data) < 2:
            break

        meta, records = data[0], data[1]
        if records is None:
            break

        for rec in records:
            if rec.get("value") is None:
                continue
            rows.append({
                "country_code": rec["country"]["id"],
                "country_name": rec["country"]["value"],
                "year":         int(rec["date"]),
                "value":        float(rec["value"]),
            })

        total_pages = meta.get("pages", 1)
        if page >= total_pages:
            break
        page += 1
        time.sleep(0.1)   # be polite to the API

    return pd.DataFrame(rows)


def fetch_country_metadata() -> pd.DataFrame:
    """Fetch country metadata: region, income group, etc."""
    rows = []
    page = 1
    per_page = 300

    while True:
        url = f"{WB_API}/country?format=json&per_page={per_page}&page={page}"
        try:
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as exc:
            print(f"  Warning: failed to fetch country metadata: {exc}")
            break

        data = resp.json()
        if not data or len(data) < 2:
            break

        meta, records = data[0], data[1]
        if records is None:
            break

        for rec in records:
            # Only keep actual countries (not aggregates)
            if rec.get("region", {}).get("id") == "NA":
                continue
            rows.append({
                "country_code":  rec["id"],
                "country_name":  rec["name"],
                "region":        rec.get("region", {}).get("value", ""),
                "income_group":  rec.get("incomeLevel", {}).get("value", ""),
                "capital_city":  rec.get("capitalCity", ""),
                "longitude":     float(rec["longitude"]) if rec.get("longitude") else None,
                "latitude":      float(rec["latitude"])  if rec.get("latitude")  else None,
            })

        total_pages = meta.get("pages", 1)
        if page >= total_pages:
            break
        page += 1

    return pd.DataFrame(rows)


def create_schema(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()

    cur.executescript("""
        DROP TABLE IF EXISTS indicator_values;
        DROP TABLE IF EXISTS indicator_metadata;
        DROP TABLE IF EXISTS countries;

        CREATE TABLE countries (
            country_code  TEXT PRIMARY KEY,
            country_name  TEXT NOT NULL,
            region        TEXT,
            income_group  TEXT,
            capital_city  TEXT,
            longitude     REAL,
            latitude      REAL
        );

        CREATE TABLE indicator_metadata (
            indicator_code  TEXT PRIMARY KEY,
            indicator_name  TEXT NOT NULL,
            category        TEXT,
            unit            TEXT
        );

        CREATE TABLE indicator_values (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            country_code    TEXT NOT NULL,
            year            INTEGER NOT NULL,
            indicator_code  TEXT NOT NULL,
            value           REAL,
            FOREIGN KEY (country_code)   REFERENCES countries(country_code),
            FOREIGN KEY (indicator_code) REFERENCES indicator_metadata(indicator_code)
        );

        CREATE INDEX IF NOT EXISTS idx_iv_country  ON indicator_values(country_code);
        CREATE INDEX IF NOT EXISTS idx_iv_year     ON indicator_values(year);
        CREATE INDEX IF NOT EXISTS idx_iv_indicator ON indicator_values(indicator_code);
    """)
    conn.commit()


def populate_indicator_metadata(conn: sqlite3.Connection) -> None:
    rows = [
        (code, name, category, unit)
        for code, (name, category, unit) in INDICATORS.items()
    ]
    conn.executemany(
        "INSERT INTO indicator_metadata(indicator_code, indicator_name, category, unit) VALUES (?,?,?,?)",
        rows,
    )
    conn.commit()


def populate_countries(conn: sqlite3.Connection, df: pd.DataFrame) -> None:
    df.to_sql("countries", conn, if_exists="replace", index=False)
    conn.commit()


def populate_indicator_values(conn: sqlite3.Connection, df: pd.DataFrame) -> None:
    df.to_sql("indicator_values", conn, if_exists="append", index=False)
    conn.commit()


def main() -> None:
    print("=" * 60)
    print("World Bank Text-to-SQL — Database Setup")
    print("=" * 60)

    if DB_PATH.exists():
        print(f"\nDatabase already exists at {DB_PATH}")
        answer = input("Re-download and overwrite? [y/N] ").strip().lower()
        if answer != "y":
            print("Aborted. Using existing database.")
            sys.exit(0)
        DB_PATH.unlink()

    conn = sqlite3.connect(DB_PATH)

    # -- Schema --
    print("\n[1/3] Creating schema...")
    create_schema(conn)
    populate_indicator_metadata(conn)
    print("      Done.")

    # -- Country metadata --
    print("\n[2/3] Fetching country metadata...")
    countries_df = fetch_country_metadata()
    print(f"      {len(countries_df)} countries retrieved.")
    populate_countries(conn, countries_df)

    # -- Indicator values --
    print(f"\n[3/3] Fetching {len(INDICATORS)} indicators ({YEAR_START}–{YEAR_END})...")
    total_rows = 0
    for i, (code, (name, category, _)) in enumerate(INDICATORS.items(), 1):
        print(f"  [{i:02d}/{len(INDICATORS)}] {name[:55]:<55} ", end="", flush=True)
        df = fetch_wb_indicator(code, YEAR_START, YEAR_END)
        if df.empty:
            print("0 rows (skipped)")
            continue
        # keep only countries we have metadata for
        valid_codes = set(countries_df["country_code"].tolist())
        df = df[df["country_code"].isin(valid_codes)].copy()
        df["indicator_code"] = code
        df.drop(columns=["country_name"], inplace=True)
        populate_indicator_values(conn, df)
        total_rows += len(df)
        print(f"{len(df):>7,} rows")

    conn.close()

    print(f"\n✓ Database written to {DB_PATH}")
    print(f"  Total indicator_values rows: {total_rows:,}")
    print("\nRun the app with:  streamlit run app.py")


if __name__ == "__main__":
    main()
