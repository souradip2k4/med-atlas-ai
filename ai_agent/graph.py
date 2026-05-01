# LangGraph
from ai_agent.tools import ALL_TOOLS
from langchain_core.runnables import RunnableLambda
# LangGraph
from langgraph.graph import END, StateGraph
from langgraph.graph.message import add_messages
from langgraph.prebuilt.tool_node import ToolNode

# Databricks imports
from databricks_langchain import ChatDatabricks

# AI Agent imports
from .config import LLM_ENDPOINT
from .prompt import SYSTEM_PROMPT
from .constants import _COLD_SPOT_KEYWORDS
import mlflow



# LangChain Core imports
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage

import json
from typing import Annotated, Any, Sequence, TypedDict

class AgentState(TypedDict):
    messages: Annotated[Sequence, add_messages]


llm = ChatDatabricks(endpoint=LLM_ENDPOINT, temperature=0.2, max_tokens=16384)
llm_with_tools = llm.bind_tools(ALL_TOOLS)


def call_model(state: AgentState) -> dict:
    messages = [SystemMessage(content=SYSTEM_PROMPT)] + list(state["messages"])
    response = llm_with_tools.invoke(messages)
    return {"messages": [response]}


def should_continue(state: AgentState) -> str:
    last = state["messages"][-1]
    if isinstance(last, AIMessage) and last.tool_calls:
        return "tools"
    return "end"


def tool_postprocessor(state: AgentState) -> dict:
    """
    Runs after every ToolNode execution. Scans the full message history to detect
    when both geospatial_query_tool and vector_search_tool results are present.

    Triggers on: GEO+SEMANTIC pipeline (Priority 1★) only.
      - Performs a Python set intersection on facility_id between the two result sets.
      - Replaces both ToolMessage contents via LangGraph's id-based in-place replacement
        (no custom reducer needed — same id = replaces existing message).
      - Both ToolMessages are kept (content replaced) to preserve the tool_call_id
        chain required by the preceding AIMessage.

    No-op for: single-tool calls, genie_chat_tool, GEO+ANALYTIC, and SEMANTIC+ANALYTIC
    (those are cascading pipelines — the LLM passes facility_ids as input to Tool B).
    """
    messages = list(state["messages"])

    # Extract only the messages from the current turn (back to the last HumanMessage)
    current_turn_msgs = []
    for msg in reversed(messages):
        if isinstance(msg, HumanMessage):
            break
        current_turn_msgs.append(msg)
    current_turn_msgs.reverse()

    # Find the MOST RECENT tool messages in the current turn
    geo_msg: ToolMessage | None = None
    vs_msg:  ToolMessage | None = None
    
    for m in reversed(current_turn_msgs):
        if isinstance(m, ToolMessage):
            # Only pick the first one we find going backwards (the most recent one)
            if m.name == "geospatial_query_tool" and geo_msg is None:
                geo_msg = m
            elif m.name == "vector_search_tool" and vs_msg is None:
                vs_msg = m

    # Only proceed when BOTH are present in the current turn
    if geo_msg is None or vs_msg is None:
        return {"messages": []}

    # Guard: skip if these specific messages were already intersected
    if ("GEO_SEMANTIC_INTERSECTION" in (geo_msg.content or "") or
        "GEO_COLDSPOT_ANALYSIS"     in (geo_msg.content or "") or
        "[Intersection performed"   in (vs_msg.content or "")):
        return {"messages": []}

    # Log raw messages to debug VS tool returns mapping
    print("\n" + "="*80)
    print("DEBUG: RAW VS_MSG CONTENT:")
    print(vs_msg.content)
    print("="*80 + "\n")

    # Parse both JSON tool outputs
    try:
        geo_data = json.loads(geo_msg.content)
        vs_data  = json.loads(vs_msg.content)
    except (json.JSONDecodeError, TypeError) as e:
        print(f"DEBUG: JSONDecodeError parsing tool outputs: {e}")
        # Unparseable — pass through unchanged, let LLM handle it as-is
        return {"messages": []}

    geo_facilities = geo_data.get("facilities", [])
    vs_results     = vs_data.get("results", [])

    # Build lookup: facility_id → geo facility record
    geo_by_id: dict[str, dict] = {
        f["facility_id"]: f
        for f in geo_facilities
        if "facility_id" in f
    }

    # Build lookup: facility_id → list of matched semantic facts (from the VS ToolMessage)
    vs_by_id: dict[str, list[dict]] = {}
    for r in vs_results:
        fid = r.get("facility_id", "")
        if fid:
            vs_by_id.setdefault(fid, []).append({
                "fact_type": r.get("fact_type", ""),
                "fact_text": r.get("fact_text", ""),
            })

    # Detect cold-spot intent from the first HumanMessage in state.
    # Cold-spot = user asking WHERE a procedure is ABSENT (not which facilities have it).
    _human_query_lower = ""
    for _msg in messages:
        if isinstance(_msg, HumanMessage):
            _human_query_lower = (_msg.content or "").lower()
            break
    _is_cold_spot = any(kw in _human_query_lower for kw in _COLD_SPOT_KEYWORDS)

    # Python set operations on facility_id
    common_ids    = set(geo_by_id.keys()) & set(vs_by_id.keys())
    cold_spot_ids = set(geo_by_id.keys()) - common_ids  # in radius, no VS procedure match

    def _build_geo_entry(fid: str) -> dict:
        """Base facility entry from geo data (lat/lon/distance already present)."""
        geo_f = geo_by_id[fid]
        return {
            "facility_id":   fid,
            "facility_name": geo_f.get("facility_name", ""),
            "facility_type": geo_f.get("facility_type", ""),
            "city":          geo_f.get("city", ""),
            "state":         geo_f.get("state", ""),
            "country":       geo_f.get("country", ""),
            "distance_km":   geo_f.get("distance_km"),
            "specialties":   geo_f.get("specialties", ""),
            "procedures":    geo_f.get("procedures", ""),
            "equipment":     geo_f.get("equipment", ""),
        }

    # Build the intersection list (same logic for BOTH cold-spot and standard 2★)
    matched: list[dict] = []
    for fid in common_ids:
        entry = _build_geo_entry(fid)
        entry["matched_facts"] = vs_by_id[fid]
        matched.append(entry)
    matched.sort(key=lambda x: x.get("distance_km") or 9999)

    if _is_cold_spot:
        # ── Cold-Spot Analysis ─────────────────────────────────────────────────
        # Same intersection as 2★, but with cold-spot metadata.
        # ONLY matched_facilities (the intersection) is passed to the LLM.
        # Cold spots are summarized as region-level counts — NOT full facility objects.
        total_in_radius = len(geo_facilities)
        coverage_pct = round(
            100 * len(matched) / total_in_radius, 1
        ) if total_in_radius > 0 else 0.0

        # Compute region-level cold-spot summary (lightweight, no per-facility detail)
        geo_regions: dict[str, int] = {}
        for fid in geo_by_id:
            region = geo_by_id[fid].get("state", "Unknown")
            geo_regions[region] = geo_regions.get(region, 0) + 1

        covered_regions: dict[str, int] = {}
        for fid in common_ids:
            region = geo_by_id[fid].get("state", "Unknown")
            covered_regions[region] = covered_regions.get(region, 0) + 1

        cold_spot_by_region: dict[str, dict] = {}
        for region, total in geo_regions.items():
            covered = covered_regions.get(region, 0)
            gap = total - covered
            if gap > 0:
                cold_spot_by_region[region] = {
                    "facilities_in_region": total,
                    "covered": covered,
                    "cold_spots": gap,
                }

        # Build top-5 nearest cold-spot facilities per region (lightweight — name/city/distance only)
        # This avoids passing all 311 cold-spot records while still giving the LLM individual facility names.
        cold_spot_by_region_facilities: dict[str, list[dict]] = {}
        for fid in cold_spot_ids:
            geo_f = geo_by_id[fid]
            region = geo_f.get("state", "Unknown") or "Unknown"
            entry = {
                "facility_name": geo_f.get("facility_name", ""),
                "facility_type": geo_f.get("facility_type", ""),
                "city":          geo_f.get("city", ""),
                "distance_km":   geo_f.get("distance_km"),
            }
            cold_spot_by_region_facilities.setdefault(region, []).append(entry)

        # Sort each region's cold-spots by ascending distance, keep top 5
        top_cold_spots_per_region: dict[str, list[dict]] = {
            region: sorted(entries, key=lambda x: x.get("distance_km") or 9999)[:5]
            for region, entries in cold_spot_by_region_facilities.items()
        }

        merged: dict = {
            "pipeline":                  "GEO_COLDSPOT_ANALYSIS",
            "geo_total":                 total_in_radius,
            "matched_count":             len(matched),
            "cold_spot_count":           len(cold_spot_ids),
            "coverage_pct":              coverage_pct,
            "radius_km":                 geo_data.get("radius_km"),
            "cold_spot_by_region":       cold_spot_by_region,
            "top_cold_spots_per_region": top_cold_spots_per_region,
            "matched_facilities":        matched,
        }
    else:
        # ── Standard GEO_SEMANTIC_INTERSECTION ────────────────────────────────
        merged = {
            "pipeline":           "GEO_SEMANTIC_INTERSECTION",
            "geo_total":          len(geo_facilities),
            "semantic_total":     len(vs_results),
            "matched_count":      len(matched),
            "reference_lat":      geo_data.get("reference_lat"),
            "reference_lon":      geo_data.get("reference_lon"),
            "radius_km":          geo_data.get("radius_km"),
            "matched_facilities": matched,
        }

    _span_name = "geo_coldspot_analysis" if _is_cold_spot else "geo_semantic_intersection"
    try:
        with mlflow.start_span(name=_span_name, span_type="CHAIN") as span:
            span.set_inputs({
                "geo_total":     len(geo_facilities),
                "semantic_total": len(vs_results),
                "radius_km":     geo_data.get("radius_km"),
                "is_cold_spot":  _is_cold_spot,
            })
            if _is_cold_spot:
                span.set_outputs({
                    "cold_spot_count":  merged["cold_spot_count"],
                    "matched_count":    merged["matched_count"],
                    "coverage_pct":     merged["coverage_pct"],
                })
            else:
                span.set_outputs({
                    "matched_count":        merged["matched_count"],
                    "matched_facility_ids": [m["facility_id"] for m in merged["matched_facilities"]],
                })
    except Exception:
        pass  # never block postprocessor execution due to tracing errors

    # Replace geo ToolMessage content with merged intersection result.
    # Using the SAME id triggers LangGraph's add_messages in-place replacement.
    geo_replacement = ToolMessage(
        id=geo_msg.id,
        tool_call_id=geo_msg.tool_call_id,
        name="geospatial_query_tool",
        content=json.dumps(merged, indent=2),
    )

    # Replace vector_search ToolMessage with a short placeholder.
    # Must be kept to satisfy the tool_call_id chain in the preceding AIMessage.
    vs_replacement = ToolMessage(
        id=vs_msg.id,
        tool_call_id=vs_msg.tool_call_id,
        name="vector_search_tool",
        content="[Intersection performed by Python postprocessor — see geospatial_query_tool result above]",
    )

    return {"messages": [geo_replacement, vs_replacement]}


def build_graph():
    graph = StateGraph(AgentState)
    graph.add_node("agent", RunnableLambda(call_model))
    graph.add_node("tools", ToolNode(ALL_TOOLS))
    graph.add_node("tool_postprocessor", RunnableLambda(tool_postprocessor))
    graph.add_conditional_edges("agent", should_continue, {"tools": "tools", "end": END})
    graph.add_edge("tools", "tool_postprocessor")
    graph.add_edge("tool_postprocessor", "agent")
    graph.set_entry_point("agent")
    return graph.compile()
