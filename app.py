"""
app.py
------
Streamlit interface for the World Bank Text-to-SQL explorer.

Run with:
    streamlit run app.py
"""

import os
import textwrap
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

from database import DB_PATH, execute_query, get_schema_description
from llm import HistoryEntry, LLMResult, question_to_sql

# ---------------------------------------------------------------------------
# Page config — must be first Streamlit call
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="World Bank Data Explorer",
    page_icon="🌍",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Custom CSS
# ---------------------------------------------------------------------------
st.markdown(
    """
    <style>
    .block-container { padding-top: 1.5rem; }
    .sql-block {
        background: #1e1e2e;
        color: #cdd6f4;
        border-radius: 6px;
        padding: 1rem 1.2rem;
        font-family: "JetBrains Mono", "Fira Code", monospace;
        font-size: 0.85rem;
        overflow-x: auto;
        white-space: pre-wrap;
        border-left: 3px solid #89b4fa;
        margin-bottom: 0.5rem;
    }
    .badge-repaired {
        background: #fff3cd; color: #856404;
        border-radius: 4px; padding: 1px 6px;
        font-size: 0.75rem; font-weight: 600;
    }
    .badge-error {
        background: #f8d7da; color: #842029;
        border-radius: 4px; padding: 1px 6px;
        font-size: 0.75rem; font-weight: 600;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


# ---------------------------------------------------------------------------
# Utility helpers  (defined before use in top-level script body)
# ---------------------------------------------------------------------------

def _escape_html(text: str) -> str:
    return (
        text.replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
    )


def _format_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Round floats to 2 decimal places for cleaner display."""
    df = df.copy()
    for col in df.select_dtypes(include="floating").columns:
        df[col] = df[col].round(2)
    return df


def _maybe_render_chart(df: pd.DataFrame) -> None:
    """
    Render an automatic chart when the result has a plottable shape:
      - 2-column result where col1 is numeric → bar or line chart
      - Time-series detected when col0 looks like a year
    """
    if df.shape[1] != 2:
        return

    label_col, value_col = df.columns[0], df.columns[1]

    if not pd.api.types.is_numeric_dtype(df[value_col]):
        return

    n_rows = len(df)
    if n_rows < 2 or n_rows > 200:
        return

    # Detect year-indexed time series
    is_time_series = str(label_col).lower() in ("year", "yr", "date") or (
        pd.api.types.is_integer_dtype(df[label_col])
        and df[label_col].between(1960, 2030).all()
    )

    chart_df = df.set_index(label_col)[[value_col]].dropna()
    if chart_df.empty:
        return

    with st.expander("Chart", expanded=True):
        if is_time_series or n_rows > 30:
            st.line_chart(chart_df, use_container_width=True)
        else:
            st.bar_chart(chart_df, use_container_width=True)


# ---------------------------------------------------------------------------
# Session state
# ---------------------------------------------------------------------------
if "history" not in st.session_state:
    st.session_state.history: list[HistoryEntry] = []

if "conversation" not in st.session_state:
    st.session_state.conversation: list[dict] = []

if "pending_question" not in st.session_state:
    st.session_state.pending_question: str = ""

if "current_result" not in st.session_state:
    st.session_state.current_result: LLMResult | None = None

if "current_question" not in st.session_state:
    st.session_state.current_question: str = ""


# ---------------------------------------------------------------------------
# Sidebar — history & settings
# ---------------------------------------------------------------------------
with st.sidebar:
    st.markdown("## 🕑 Query History")

    if not st.session_state.history:
        st.caption("Your queries will appear here.")
    else:
        for i, entry in enumerate(reversed(st.session_state.history)):
            idx = len(st.session_state.history) - 1 - i
            label = textwrap.shorten(entry.question, width=52, placeholder="…")

            if st.button(label, key=f"hist_{idx}", use_container_width=True, help=entry.question):
                st.session_state.pending_question = entry.question
                st.rerun()

            if entry.repaired:
                st.markdown('<span class="badge-repaired">auto-repaired</span>', unsafe_allow_html=True)
            elif entry.error:
                st.markdown('<span class="badge-error">error</span>', unsafe_allow_html=True)

        if st.button("Clear history", use_container_width=True, type="secondary"):
            st.session_state.history        = []
            st.session_state.conversation   = []
            st.session_state.current_result = None
            st.session_state.current_question = ""
            st.rerun()

    st.divider()

    st.markdown("## ⚙️ Settings")
    api_key_input = st.text_input(
        "Anthropic API Key",
        type="password",
        value=os.environ.get("ANTHROPIC_API_KEY", ""),
        help="Used only within this session; never stored.",
    )
    if api_key_input:
        os.environ["ANTHROPIC_API_KEY"] = api_key_input

    st.divider()
    st.markdown("## 📊 About the data")
    st.caption(
        "**Source:** World Bank World Development Indicators  \n"
        "**Coverage:** ~217 countries · 26 indicators · 2000–2023  \n"
        "**Licence:** CC BY 4.0  \n"
        "[data.worldbank.org](https://data.worldbank.org/)"
    )

    with st.expander("Schema reference"):
        try:
            st.code(get_schema_description(), language="text")
        except Exception:
            st.warning("Database not set up yet — run setup_database.py first.")


# ---------------------------------------------------------------------------
# Main area
# ---------------------------------------------------------------------------
st.title("🌍 World Bank Data Explorer")
st.markdown(
    "Ask a question in plain English and get an instant SQL query + live results "
    "from the [World Development Indicators](https://data.worldbank.org/) database."
)

# DB health check
if not DB_PATH.exists():
    st.error(
        "**Database not found.**  \n"
        "Run `python setup_database.py` in your terminal to download the "
        "World Bank data (≈5 min, requires internet).",
        icon="🗄️",
    )
    st.stop()

# ---------------------------------------------------------------------------
# Example question chips
# ---------------------------------------------------------------------------
EXAMPLES = [
    "Which 10 countries had the highest GDP per capita in 2022?",
    "Show life expectancy trends for Germany, Japan, and Brazil from 2000 to 2020.",
    "Average CO₂ emissions per capita by income group in 2019.",
    "Which Sub-Saharan African countries reduced infant mortality the most since 2000?",
    "Compare internet usage rates across regions in 2022.",
    "Countries where unemployment exceeded 15% in 2020.",
    "Top 10 countries by health expenditure as % of GDP in the latest year.",
    "Show Gini index for Latin American countries in the most recent year.",
    "How has urban population share changed in South Asia since 2005?",
    "Relationship between GDP per capita and literacy rate in 2018.",
]

st.markdown("#### Try an example")
example_cols = st.columns(5)
for j, ex in enumerate(EXAMPLES):
    if example_cols[j % 5].button(
        textwrap.shorten(ex, 38, placeholder="…"),
        key=f"ex_{j}",
        help=ex,
        use_container_width=True,
    ):
        st.session_state.pending_question = ex
        st.rerun()

st.divider()

# ---------------------------------------------------------------------------
# Input form
# ---------------------------------------------------------------------------
with st.form("question_form", clear_on_submit=False):
    question = st.text_area(
        "Your question",
        value=st.session_state.pending_question,
        height=80,
        placeholder="e.g. Which 5 countries have the lowest infant mortality rate in 2021?",
        label_visibility="collapsed",
    )
    btn_col, ctx_col = st.columns([1, 4])
    submitted   = btn_col.form_submit_button("Generate SQL ▶", type="primary", use_container_width=True)
    use_context = ctx_col.checkbox(
        "Use conversation context (enables follow-up questions)",
        value=True,
    )

# Consume the pending question so it doesn't loop
if st.session_state.pending_question:
    st.session_state.pending_question = ""

# ---------------------------------------------------------------------------
# Run inference
# ---------------------------------------------------------------------------
if submitted and question.strip():
    with st.spinner("Generating SQL and querying the database…"):
        conv = st.session_state.conversation if use_context else []
        result = question_to_sql(question.strip(), conversation_history=conv)

    st.session_state.current_question = question.strip()
    st.session_state.current_result   = result

    # Maintain multi-turn context
    if use_context:
        st.session_state.conversation.append({"role": "user",      "content": question.strip()})
        if result.sql:
            st.session_state.conversation.append({"role": "assistant", "content": result.sql})

    # Save to history
    st.session_state.history.append(
        HistoryEntry(
            question=question.strip(),
            sql=result.sql,
            result=result.result,
            error=result.error,
            repaired=result.repaired,
        )
    )
    st.rerun()

elif submitted and not question.strip():
    st.warning("Please enter a question first.")


# ---------------------------------------------------------------------------
# Display results
# ---------------------------------------------------------------------------
res: LLMResult | None = st.session_state.current_result
cur_q: str            = st.session_state.current_question

if res is not None:
    st.markdown(f"### Results — *{cur_q}*")

    # SQL panel
    if res.sql:
        with st.expander("Generated SQL", expanded=True):
            if res.repaired:
                st.info(
                    "The first SQL attempt raised a database error. "
                    "The query was automatically corrected and re-run.",
                    icon="⚡",
                )
            st.markdown(
                f'<div class="sql-block">{_escape_html(res.sql)}</div>',
                unsafe_allow_html=True,
            )

    # Error panel
    if res.error:
        st.error(
            f"**Query failed:** {res.error}  \n\n"
            "**Suggestions:**\n"
            "- Rephrase the question with more specific country names or years.\n"
            "- Check the *Schema reference* in the sidebar for valid indicator names.\n"
            "- Some indicators have sparse coverage — try a different year.",
            icon="❌",
        )

    # Data panel
    if res.success and res.result is not None and res.result.df is not None:
        df = res.result.df

        if res.result.truncated:
            st.warning(
                f"Result truncated to {len(df):,} rows. "
                "Add a LIMIT clause or narrow your query for the full dataset.",
                icon="⚠️",
            )

        meta_col, dl_col = st.columns([3, 1])
        meta_col.caption(f"{len(df):,} {'row' if len(df) == 1 else 'rows'} returned")

        csv_bytes = df.to_csv(index=False).encode("utf-8")
        dl_col.download_button(
            "⬇ Download CSV",
            data=csv_bytes,
            file_name=f"wdi_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
            use_container_width=True,
        )

        if len(df) == 0:
            st.info(
                "The query ran successfully but returned no rows.  \n"
                "The indicator or year may have no reported data — try a different year.",
                icon="ℹ️",
            )
        else:
            st.dataframe(_format_dataframe(df), use_container_width=True, hide_index=True)
            _maybe_render_chart(df)
