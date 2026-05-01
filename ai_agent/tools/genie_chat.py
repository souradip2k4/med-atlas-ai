# ─── Tool 1 — Genie Chat ──────────────────────────────────────────────────────

import warnings
from ai_agent.config import GENIE_ID
from langchain.tools import tool

@tool
def genie_chat_tool(query: str) -> str:
    """
    Route quantitative, aggregation, and SQL-friendly questions to the Genie Space.

    Best for: facility counts, region/district/state statistics, averages, rankings,
    "how many", "total", "most", "least", "top N", "number of", "how many hospitals in",
    structured column filtering, comparisons, distributions, bed/staff ratios.

    Trigger keywords: "how many", "count", "total", "average", "sum", "most",
    "least", "top N", "region", "district", "state", "ownership", "beds", "capacity",
    "staff", "ratio", "percentage", "ranking", "compar", "distribution".

    NOT for: semantic similarity, free-text capability searches, facility details.
    """
    from databricks_langchain import GenieAgent

    enhanced_query = (
        f"{query}\n\n"
        "---\n"
        "IMPORTANT SCHEMA INSTRUCTIONS FOR GENIE:\n"
        "The `regional_insights` table is pre-aggregated and sliced. You MUST always filter by `insight_category` to avoid double-counting:\n"
        "1. To get absolute total facility numbers, you MUST use `WHERE insight_category = 'overview' AND insight_value = 'all_facilities'`.\n"
        "2. To group or count by operator type (public vs private), explicitly use `WHERE insight_category = 'operator'`.\n"
        "3. To group or count by medical specialty, explicitly use `WHERE insight_category = 'specialty'`.\n"
        "4. If querying the `facility_records` table directly instead, strictly use `COUNT(facility_id)` for totals."
    )

    try:
        agent = GenieAgent(GENIE_ID)
        response = agent.invoke({"messages": [{"role": "user", "content": enhanced_query}]})
        return response["messages"][-1].content if "messages" in response else str(response)
    except AttributeError as exc:
        # MLflow tracing can raise internal LiveSpan/trace_id AttributeErrors.
        # Retry once — the second attempt succeeds without tracing interference.
        if "trace_id" in str(exc) or "LiveSpan" in str(exc):
            warnings.warn(f"Tracing internal error (non-fatal), retrying: {exc}")
            agent = GenieAgent(GENIE_ID)
            response = agent.invoke({"messages": [{"role": "user", "content": enhanced_query}]})
            return response["messages"][-1].content if "messages" in response else str(response)
        raise

