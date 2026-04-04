"""
database.py
-----------
SQLite interface: schema introspection, safe query execution,
and the schema-description string used to ground the LLM prompt.
"""

import sqlite3
import textwrap
from pathlib import Path
from typing import Optional

import pandas as pd

DB_PATH = Path(__file__).parent / "world_bank.db"

# Maximum rows returned to the UI to prevent runaway result sets
MAX_ROWS = 500


# ---------------------------------------------------------------------------
# Connection helpers
# ---------------------------------------------------------------------------

def get_connection() -> sqlite3.Connection:
    """Return a read-only (uri mode) connection to the database."""
    if not DB_PATH.exists():
        raise FileNotFoundError(
            f"Database not found at {DB_PATH}. "
            "Run  python setup_database.py  first."
        )
    # uri=True + ?mode=ro prevents accidental writes from LLM-generated SQL
    uri = f"file:{DB_PATH}?mode=ro"
    conn = sqlite3.connect(uri, uri=True, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


# ---------------------------------------------------------------------------
# Query execution
# ---------------------------------------------------------------------------

class QueryResult:
    """Container returned by execute_query."""

    def __init__(
        self,
        df: Optional[pd.DataFrame] = None,
        error: Optional[str] = None,
        truncated: bool = False,
        row_count: int = 0,
    ):
        self.df        = df
        self.error     = error
        self.truncated = truncated
        self.row_count = row_count

    @property
    def success(self) -> bool:
        return self.error is None


def execute_query(sql: str) -> QueryResult:
    """
    Execute *sql* against the local SQLite database and return a QueryResult.

    Enforces:
    - Read-only connection (no INSERT / UPDATE / DELETE / DROP)
    - Row-count cap (MAX_ROWS)
    - Timeout via SQLite progress handler isn't available in the stdlib,
      so we rely on the read-only flag and row cap for safety.
    """
    # Lightweight guard against write statements — the connection is already
    # read-only at the driver level, but we provide a clear error message.
    forbidden = ("insert ", "update ", "delete ", "drop ", "create ",
                 "alter ", "attach ", "pragma ")
    sql_lower = sql.strip().lower()
    for kw in forbidden:
        if sql_lower.startswith(kw) or f" {kw}" in sql_lower:
            return QueryResult(
                error=f"Write operation '{kw.strip()}' is not permitted. "
                      "Only SELECT queries are allowed."
            )

    try:
        conn = get_connection()
        # Use pandas for convenient DataFrame conversion
        df = pd.read_sql_query(sql, conn, dtype_backend="numpy_nullable")
        conn.close()
    except FileNotFoundError as exc:
        return QueryResult(error=str(exc))
    except Exception as exc:
        # Translate SQLite errors into a user-readable message
        return QueryResult(error=_friendly_sqlite_error(str(exc)))

    truncated = len(df) > MAX_ROWS
    if truncated:
        df = df.head(MAX_ROWS)

    return QueryResult(df=df, truncated=truncated, row_count=len(df))


def _friendly_sqlite_error(raw: str) -> str:
    """Map common SQLite error strings to clearer messages."""
    raw_l = raw.lower()
    if "no such table" in raw_l:
        table = raw.split(":")[-1].strip()
        return (
            f"Table not found: {table}. "
            "Valid tables are: countries, indicator_values, indicator_metadata."
        )
    if "no such column" in raw_l:
        return f"Column not found — {raw}. Check column names in the schema."
    if "syntax error" in raw_l:
        return f"SQL syntax error — {raw}."
    if "readonly database" in raw_l:
        return "Write operations are not permitted."
    return raw


# ---------------------------------------------------------------------------
# Schema description (fed into the LLM system prompt)
# ---------------------------------------------------------------------------

def get_schema_description() -> str:
    """
    Return a detailed, LLM-friendly description of the database schema,
    including sample indicator codes and statistical caveats.
    """
    try:
        conn = get_connection()
        cur  = conn.cursor()

        # Fetch all indicator codes with names and categories
        cur.execute("""
            SELECT indicator_code, indicator_name, category, unit
            FROM indicator_metadata
            ORDER BY category, indicator_name
        """)
        indicators = cur.fetchall()

        # Fetch distinct regions
        cur.execute("SELECT DISTINCT region FROM countries WHERE region != '' ORDER BY region")
        regions = [r[0] for r in cur.fetchall()]

        # Fetch distinct income groups
        cur.execute("SELECT DISTINCT income_group FROM countries WHERE income_group != '' ORDER BY income_group")
        income_groups = [r[0] for r in cur.fetchall()]

        # Fetch year range
        cur.execute("SELECT MIN(year), MAX(year) FROM indicator_values")
        year_min, year_max = cur.fetchone()

        # Fetch total country count
        cur.execute("SELECT COUNT(*) FROM countries")
        n_countries = cur.fetchone()[0]

        conn.close()
    except Exception:
        # Fallback if DB not yet set up (schema description still needed at import)
        indicators, regions, income_groups = [], [], []
        year_min, year_max, n_countries = 2000, 2023, 217

    indicators_block = "\n".join(
        f"    {row[0]:<32} -- {row[1]} [{row[3]}] (category: {row[2]})"
        for row in indicators
    )
    regions_block     = "\n".join(f"    {r}" for r in regions)
    income_group_block = "\n".join(f"    {g}" for g in income_groups)

    return textwrap.dedent(f"""
    ## Database: World Bank World Development Indicators (SQLite)
    ## Source: World Bank Open Data  https://data.worldbank.org/
    ## Coverage: {n_countries} countries/territories, years {year_min}–{year_max}

    ### Tables

    #### countries
    Dimension table — one row per country/territory.
    Columns:
      country_code  TEXT  -- ISO 3166-1 alpha-2 code (e.g. "US", "DE", "IN")
      country_name  TEXT  -- Full English name
      region        TEXT  -- World Bank region grouping
      income_group  TEXT  -- World Bank income classification
      capital_city  TEXT
      longitude     REAL
      latitude      REAL

    Distinct regions:
{regions_block}

    Distinct income groups:
{income_group_block}

    #### indicator_metadata
    Reference table — one row per indicator.
    Columns:
      indicator_code  TEXT  -- World Bank indicator code (PRIMARY KEY)
      indicator_name  TEXT  -- Human-readable name
      category        TEXT  -- Thematic category
      unit            TEXT  -- Measurement unit

    #### indicator_values  (fact table)
    Columns:
      id              INTEGER  -- surrogate key
      country_code    TEXT     -- FK → countries.country_code
      year            INTEGER  -- calendar year
      indicator_code  TEXT     -- FK → indicator_metadata.indicator_code
      value           REAL     -- indicator value (NULL if not reported)

    ### Available indicator codes
{indicators_block}

    ### Query patterns

    -- 1. Single indicator for one country over time
    SELECT year, value
    FROM indicator_values
    WHERE country_code = 'DE'
      AND indicator_code = 'NY.GDP.PCAP.CD'
    ORDER BY year;

    -- 2. Cross-country comparison for a single year
    SELECT c.country_name, iv.value
    FROM indicator_values iv
    JOIN countries c ON c.country_code = iv.country_code
    WHERE iv.indicator_code = 'SP.DYN.LE00.IN'
      AND iv.year = 2020
    ORDER BY iv.value DESC;

    -- 3. Regional averages
    SELECT c.region, AVG(iv.value) AS avg_value
    FROM indicator_values iv
    JOIN countries c ON c.country_code = iv.country_code
    WHERE iv.indicator_code = 'NY.GDP.PCAP.CD'
      AND iv.year = 2022
    GROUP BY c.region
    ORDER BY avg_value DESC;

    -- 4. Top-N countries
    SELECT c.country_name, iv.value
    FROM indicator_values iv
    JOIN countries c ON c.country_code = iv.country_code
    WHERE iv.indicator_code = 'EN.ATM.CO2E.PC'
      AND iv.year = 2020
      AND iv.value IS NOT NULL
    ORDER BY iv.value DESC
    LIMIT 10;

    -- 5. Two indicators joined (e.g. GDP vs life expectancy)
    SELECT c.country_name,
           gdp.value AS gdp_per_capita,
           le.value  AS life_expectancy
    FROM countries c
    JOIN indicator_values gdp ON gdp.country_code = c.country_code
                              AND gdp.indicator_code = 'NY.GDP.PCAP.CD'
    JOIN indicator_values le  ON le.country_code  = c.country_code
                              AND le.indicator_code = 'SP.DYN.LE00.IN'
                              AND le.year = gdp.year
    WHERE gdp.year = 2020
      AND gdp.value IS NOT NULL
      AND le.value  IS NOT NULL
    ORDER BY gdp.value DESC;

    ### Statistical notes
    - Values are NULL when not reported by the country for that year.
      Use  AND value IS NOT NULL  to exclude missing data.
    - GDP and GNI figures are in *current* (nominal) US dollars unless the
      indicator name specifies otherwise (e.g. PPP or constant prices).
    - The Gini index ranges 0 (perfect equality) to 100 (maximum inequality).
    - Poverty headcount ratio uses the $2.15/day 2017 PPP threshold.
    - Coverage varies by indicator; education and poverty indicators have
      more gaps than GDP or population series.
    - "country_code" values are ISO alpha-2 strings: use 'US', 'GB', 'CN', etc.
    """).strip()
