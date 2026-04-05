"""
llm.py
------
Translates natural-language questions into SQL using the Anthropic Claude API.

Design notes
~~~~~~~~~~~~
* We use a two-stage approach: generate → validate → (optionally) repair.
  If Claude's first SQL attempt raises a SQLite error, we send the error back
  for a single repair attempt before surfacing the failure to the user.
* The system prompt embeds the full schema description so the model has
  precise knowledge of table names, column names, indicator codes, and
  statistical caveats — reducing hallucination significantly.
* Conversation history is passed as part of the messages list so that
  follow-up questions ("now show just Sub-Saharan Africa") resolve naturally.
"""

import os
import re
import textwrap
from dataclasses import dataclass, field
from typing import Optional

import anthropic

from database import execute_query, get_schema_description, QueryResult

MODEL = "claude-sonnet-4-6"

# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT_TEMPLATE = """\
You are an expert data analyst and SQL engineer specialising in World Bank \
development economics data.

Your job is to translate a user's natural-language question into a single, \
correct SQLite SELECT query, then return ONLY the SQL — no explanation, \
no markdown fences, no surrounding text.

Rules:
1. Return ONLY the raw SQL statement ending with a semicolon.
2. Never use DML (INSERT, UPDATE, DELETE) or DDL (CREATE, DROP, ALTER).
3. Always include NULL checks (AND value IS NOT NULL) when aggregating.
4. Prefer readable column aliases (e.g. AS country, AS gdp_per_capita).
5. When the user asks for a "top" list without specifying a year, use the
   most recent year that has data (approach: sub-select MAX(year)).
6. Country codes are ISO alpha-2 strings ('US', 'GB', 'CN', etc.).
7. The database dialect is SQLite — use SQLite-compatible functions.
8. If the question is ambiguous, make a reasonable data-analyst choice and
   note it only as a SQL comment at the top (e.g. -- using year 2022).

{schema}
"""

_REPAIR_INSTRUCTION = textwrap.dedent("""
    The SQL you generated produced this error:

        {error}

    Original SQL:
        {sql}

    Diagnose the error and return a corrected SQL query (raw SQL only, \
    no markdown).
""")


# ---------------------------------------------------------------------------
# Public data structures
# ---------------------------------------------------------------------------

@dataclass
class LLMResult:
    sql:      Optional[str]   = None
    result:   Optional[QueryResult] = None
    error:    Optional[str]   = None
    repaired: bool            = False   # True if the repair pass was needed

    @property
    def success(self) -> bool:
        return self.error is None and self.result is not None and self.result.success


@dataclass
class HistoryEntry:
    question:  str
    sql:       Optional[str]
    result:    Optional[QueryResult]
    error:     Optional[str]
    repaired:  bool = False


# ---------------------------------------------------------------------------
# Core translation function
# ---------------------------------------------------------------------------

def question_to_sql(
    question: str,
    conversation_history: list[dict] | None = None,
) -> LLMResult:
    """
    Translate *question* to SQL, execute it, and return an LLMResult.

    Parameters
    ----------
    question:
        The user's natural-language question.
    conversation_history:
        Optional list of prior {"role": ..., "content": ...} messages
        for multi-turn context.
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        return LLMResult(
            error="ANTHROPIC_API_KEY environment variable is not set."
        )

    client = anthropic.Anthropic(api_key=api_key)
    schema = get_schema_description()
    system_prompt = _SYSTEM_PROMPT_TEMPLATE.format(schema=schema)

    # Build message list (prior turns + current question)
    messages: list[dict] = list(conversation_history or [])
    messages.append({"role": "user", "content": question})

    # ----------------------------------------------------------------
    # Stage 1: generate SQL
    # ----------------------------------------------------------------
    try:
        response = client.messages.create(
            model=MODEL,
            max_tokens=1024,
            system=system_prompt,
            messages=messages,
        )
    except anthropic.AuthenticationError:
        return LLMResult(error="Invalid Anthropic API key. Check ANTHROPIC_API_KEY.")
    except anthropic.RateLimitError:
        return LLMResult(error="Anthropic API rate limit reached. Please wait and retry.")
    except anthropic.APIError as exc:
        return LLMResult(error=f"Anthropic API error: {exc}")

    sql = _extract_sql(response.content[0].text)
    if not sql:
        return LLMResult(
            error="The model did not return a SQL query. Try rephrasing the question."
        )

    # ----------------------------------------------------------------
    # Stage 2: execute
    # ----------------------------------------------------------------
    qr = execute_query(sql)
    if qr.success:
        return LLMResult(sql=sql, result=qr)

    # ----------------------------------------------------------------
    # Stage 3: repair — send error back once and retry
    # ----------------------------------------------------------------
    repair_prompt = _REPAIR_INSTRUCTION.format(error=qr.error, sql=sql)
    repair_messages = messages + [
        {"role": "assistant", "content": sql},
        {"role": "user",      "content": repair_prompt},
    ]

    try:
        repair_response = client.messages.create(
            model=MODEL,
            max_tokens=1024,
            system=system_prompt,
            messages=repair_messages,
        )
    except anthropic.APIError as exc:
        # Return the original SQL error, not the API error
        return LLMResult(sql=sql, error=qr.error)

    repaired_sql = _extract_sql(repair_response.content[0].text)
    if not repaired_sql:
        return LLMResult(sql=sql, error=qr.error)

    repaired_qr = execute_query(repaired_sql)
    if repaired_qr.success:
        return LLMResult(sql=repaired_sql, result=repaired_qr, repaired=True)

    # Both attempts failed — surface the more informative error
    best_error = repaired_qr.error or qr.error
    return LLMResult(sql=repaired_sql, error=best_error)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _extract_sql(text: str) -> Optional[str]:
    """
    Extract a SQL statement from the model's raw text output.

    Handles three common formats:
      1. ```sql ... ```
      2. ``` ... ```
      3. Plain text (the model followed instructions correctly)
    """
    if not text:
        return None

    text = text.strip()

    # Try fenced code block first
    fenced = re.search(r"```(?:sql)?\s*(.*?)```", text, re.DOTALL | re.IGNORECASE)
    if fenced:
        sql = fenced.group(1).strip()
        return sql if sql else None

    # Fall back: look for SELECT keyword (handles plain-text responses)
    select_match = re.search(r"(SELECT\b.*)", text, re.DOTALL | re.IGNORECASE)
    if select_match:
        return select_match.group(1).strip()

    # The model may have returned raw SQL with a comment at the top
    if re.match(r"\s*(--|SELECT|WITH)\s", text, re.IGNORECASE):
        return text

    return None
