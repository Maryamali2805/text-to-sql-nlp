# World Bank Text-to-SQL Explorer

A natural-language interface to the World Bank World Development Indicators,
powered by Claude (Anthropic) for SQL generation and SQLite for data storage.

Ask questions like:

> *"Which 10 countries had the highest GDP per capita in 2022?"*  
> *"Show infant mortality trends for Sub-Saharan Africa since 2000."*  
> *"Compare CO₂ emissions per capita across income groups in 2019."*

---

## Architecture

```
User question
     │
     ▼
 llm.py  ──  Claude (claude-sonnet-4-6)
             System prompt: full schema description + statistical notes
             └─► raw SQL
                   │
                   ▼
             database.py  ──  SQLite (read-only)
             world_bank.db
                   │
                   ▼
             pandas DataFrame
                   │
                   ▼
              app.py  ──  Streamlit UI
```

### Two-stage SQL generation with automatic repair

1. **Generate** — Claude receives the schema description and the user's question,
   returns a SQL query.
2. **Execute** — SQLite runs the query.
3. **Repair (if needed)** — If execution fails, the error is sent back to Claude
   for a single correction attempt before surfacing the failure to the user.

---

## Data

**Source:** [World Bank World Development Indicators](https://data.worldbank.org/)  
**Licence:** [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/)  
**Coverage:** ~217 countries/territories · 26 curated indicators · 2000–2023

### Indicators included

| Category      | Examples |
|---------------|---------|
| Economic      | GDP per capita, GNI per capita, inflation, exports, government debt |
| Demographics  | Population, growth rate, urban share, fertility rate |
| Health        | Life expectancy, infant mortality, health expenditure, undernourishment |
| Education     | Literacy rate, government education expenditure, primary enrolment |
| Environment   | CO₂ emissions, energy use, forest area |
| Technology    | Internet users, mobile subscriptions |
| Poverty       | $2.15/day poverty headcount, Gini index |
| Labor         | Unemployment, female labour force participation |

### Database schema

```sql
countries (
    country_code TEXT PRIMARY KEY,  -- ISO alpha-2 e.g. "US", "DE"
    country_name TEXT,
    region TEXT,
    income_group TEXT,
    capital_city TEXT,
    longitude REAL, latitude REAL
)

indicator_metadata (
    indicator_code TEXT PRIMARY KEY,  -- e.g. "NY.GDP.PCAP.CD"
    indicator_name TEXT,
    category TEXT,
    unit TEXT
)

indicator_values (
    id INTEGER PRIMARY KEY,
    country_code TEXT,   -- FK → countries
    year INTEGER,
    indicator_code TEXT, -- FK → indicator_metadata
    value REAL
)
```

---

## Setup

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Download the World Bank data (~5 min)

```bash
python setup_database.py
```

This fetches data from the public World Bank API and writes `world_bank.db`
(~50–80 MB). The script is idempotent; re-running asks before overwriting.

### 3. Set your Anthropic API key

```bash
export ANTHROPIC_API_KEY=sk-ant-...
```

Or enter it directly in the app sidebar.

### 4. Run the app

```bash
streamlit run app.py
```

---

## Features

- **Natural language → SQL** via Claude with a schema-aware system prompt
- **Auto-repair** — failed queries are sent back to the model for correction
- **Multi-turn context** — follow-up questions resolve naturally ("now show
  just Europe", "sort by population instead")
- **Query history panel** in the sidebar with one-click re-run
- **Automatic charts** — time-series → line chart; categorical → bar chart
- **CSV download** for every result set
- **Read-only SQLite connection** — LLM-generated SQL cannot modify the data

---

## Design notes

Several choices reflect the statistical nature of the data:

- **NULL handling** — the system prompt instructs the model always to include
  `AND value IS NOT NULL` when aggregating, because WDI coverage is uneven.
- **Nominal vs real values** — GDP and GNI indicators are nominal (current USD)
  by default; the prompt warns the model to respect this and use the correct
  indicator code when the user asks for inflation-adjusted figures.
- **Gini and poverty** — statistical notes in the schema description explain
  the scale and PPP threshold so the model does not misinterpret ranges.
- **"Most recent year"** — when no year is specified, the model is instructed
  to sub-select `MAX(year)` rather than hard-coding a year that may have no data.
