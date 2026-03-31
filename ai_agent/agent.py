"""
Med-Atlas-AI LangGraph Agent
============================
A healthcare infrastructure Q&A agent for Ghanaian medical facilities.

Tools:
  1. genie_chat_tool         — Natural language → SQL via Genie Space
  2. vector_search_tool      — Semantic search on facility_facts (VS with fact_type filter)
  3. medical_agent_tool      — Statistical anomaly detection via analyze_medical_query UC function
  4. geospatial_query_tool   — Distance-based facility search via find_facilities_nearby UC function

Architecture:
  - Single LangGraph graph: [agent] → [tools] → [agent]
  - LLM decides which tool(s) to call based on query type
  - ResponsesAgent pattern for MLflow deployment compatibility
"""

import json
import uuid
import warnings
import mlflow
import os
from pathlib import Path
from typing import Annotated, Any, Generator, Sequence, TypedDict

from dotenv import load_dotenv
_env_path = Path(__file__).parent.parent / ".env"
load_dotenv(_env_path)

os.environ["DATABRICKS_DISABLE_NOTICE"] = "true"
mlflow.set_tracking_uri("sqlite:///mlflow.db")
# Enable LangChain tracing so tool calls and LLM responses are captured in MLflow.
mlflow.langchain.autolog()

# LangGraph
from langgraph.graph import END, StateGraph
from langgraph.graph.message import add_messages
from langgraph.prebuilt.tool_node import ToolNode

# LangChain
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage
from langchain_core.runnables import RunnableLambda
from langchain_core.tools import tool

# MLflow ResponsesAgent (MLflow 3.x)
from mlflow.pyfunc import ResponsesAgent
from mlflow.types.responses import (
    ResponsesAgentRequest,
    ResponsesAgentResponse,
    ResponsesAgentStreamEvent,
    ResponseOutputItemDoneEvent,
    OutputItem,
    to_chat_completions_input,
)
from mlflow.types.responses_helpers import (
    Content,
    ResponseOutputText,
)

# Databricks integrations
from databricks_langchain import ChatDatabricks

# ─── Configuration ─────────────────────────────────────────────────────────────

LLM_ENDPOINT  = os.environ["LLM_ENDPOINT"]
VS_INDEX     = os.environ.get("VS_INDEX", "med_atlas_ai.default.facility_facts_index")
GENIE_ID     = os.environ["GENIE_SPACE_ID"]
CATALOG      = os.environ.get("CATALOG", "med_atlas_ai")
SCHEMA       = os.environ.get("SCHEMA", "default")


# ─── Tool 1 — Genie Chat ──────────────────────────────────────────────────────

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

    try:
        agent = GenieAgent(GENIE_ID)
        return agent.invoke({"messages": [{"role": "user", "content": query}]})
    except AttributeError as exc:
        # MLflow tracing can raise internal LiveSpan/trace_id AttributeErrors.
        # Retry once — the second attempt succeeds without tracing interference.
        if "trace_id" in str(exc) or "LiveSpan" in str(exc):
            warnings.warn(f"Tracing internal error (non-fatal), retrying: {exc}")
            agent = GenieAgent(GENIE_ID)
            return agent.invoke({"messages": [{"role": "user", "content": query}]})
        raise


# ─── Tool 2 — Vector Search ───────────────────────────────────────────────────

@tool
def vector_search_tool(query: str, fact_types: list[str] | str | None = None) -> str:
    """
    Semantic search over pre-generated facility facts.

    Best for: "Which facilities provide cardiac surgery?", "has MRI?",
    "similar to [name]", specialized services, capabilities, equipment.

    Args:
        query:      Natural language search query
        fact_types: Optional filter to specific fact types.
                    Valid values: procedure, capability, specialty, summary, equipment.
                    Pass a list like ["procedure"] or a single string like "procedure".
                    If None, searches across all fact types.
    """
    from databricks_langchain import VectorSearchRetrieverTool

    if isinstance(fact_types, str):
        fact_types = [fact_types]

    kwargs = {
        "index_name": VS_INDEX,
        "num_results": 15,
        "columns": ["fact_id", "facility_id", "fact_text", "fact_type", "source_text"],
    }

    if fact_types and len(fact_types) == 1:
        kwargs["filters"] = {"fact_type": fact_types[0]}
    elif fact_types and len(fact_types) > 1:
        kwargs["filters"] = {"fact_type": {"$in": fact_types}}

    post_filter_types = set(fact_types) if fact_types and len(fact_types) > 1 else None

    try:
        vs = VectorSearchRetrieverTool(**kwargs)
        results = vs.invoke({"query": query})
        if post_filter_types and isinstance(results, list):
            results = [
                doc for doc in results
                if doc.metadata.get("fact_type") in post_filter_types
            ]
        return results
    except Exception as exc:
        return f"[Vector Search Error] {exc}"


# ─── Tool 3 — Medical Agent ───────────────────────────────────────────────────

@tool
def medical_agent_tool(query: str, facility_id: str | None = None) -> str:
    """
    Medical domain reasoning and anomaly detection on facility data.

    Uses the analyze_medical_query UC function on facility_records and
    facility_facts tables to detect data quality issues and anomalies.

    Returns data for 8 analysis types:
      1. reliability_score        — 0-100 data quality score based on fact density
      2. regional_coverage        — per-region service coverage arrays for LLM gap analysis
      3. duplicate_facility       — exact same facility name occurring multiple times
      4. anomaly_flagging         — outlier capacity/doctor counts (3 std devs)
      5. feature_mismatch_raw     — raw procedure vs equipment counts for LLM plausibility check
      6. ngo_overlap_raw          — NGOs grouped by affiliation+region for LLM overlap analysis
      7. facility_profile_counts  — raw per-facility counts for LLM gap classification
      8. data_staleness           — outdated records (updated_at age scoring)

    NOTE — For classification/breakdown queries (facility_type, operator_type, affiliation_types,
      ngo counts, public vs private breakdown) — use genie_chat_tool instead.
      These are enum fields; Genie handles them with simple GROUP BY aggregations.

    NOTE — For contradiction detection, use vector_search_tool instead:
      Contradictions/inconsistencies are handled dynamically via semantic search,
      not by this tool. Route 'contradict', 'inconsistent' queries to vector_search_tool.

    IMPORTANT — For branches that return raw data (types 2, 4, 7, 8, 9), YOU must:
      • Read the 'note' field in each finding and apply medical/domain reasoning
      • Classify NGO levels, identify service gaps, assess overlap, or classify gap types
      • Evaluate if procedure-to-equipment ratios are medically implausible
      • Do NOT just echo the raw data — synthesize a meaningful analysis

    Routed to genie_chat_tool instead (NOT this tool):
      - Oversupply/scarcity queries → genie_chat_tool (e.g., "how many facilities offer X?")
      - Specialist distribution    → genie_chat_tool (queries regional_insights table directly)
      - Web/description quality    → genie_chat_tool (e.g., "which facilities have websites?")

    Trigger keywords: "anomal", "inconsisten", "contradict", "reliab", "score",
    "quality", "ngo", "classify", "gap", "unmet", "outlier", "flag",
    "duplicate", "abnormal", "red flag", "problem type", "workforce",
    "staffing", "overlapping", "staleness", "stale", "corrobor",
    "mismatch", "feature mismatch", "procedure count", "equipment count".

    NOT for: oversupply, scarcity, specialist distribution, web presence → use genie_chat_tool.

    Args:
        query:      Analysis question (e.g., "detect anomalies", "score reliability")
        facility_id: Optional. Restrict analysis to one facility.

    Returns: Structured JSON with findings + optional 'note' fields for LLM reasoning.
    """
    from unitycatalog.ai.langchain.toolkit import UCFunctionToolkit

    args = {"query": query}
    if facility_id:
        args["facility_id"] = facility_id

    try:
        uc = UCFunctionToolkit(
            function_names=[f"{CATALOG}.{SCHEMA}.analyze_medical_query"]
        )
        uc_fn = uc.tools[0]
        raw_result = uc_fn.invoke({"query_json": json.dumps(args)})
        outer = json.loads(raw_result)
        findings_raw = outer.get("findings", "[]")
        findings = json.loads(findings_raw)
        outer["findings"] = findings
        return json.dumps(outer, indent=2)
    except Exception as exc:
        return f"[Medical Agent Error] {exc}"


# ─── Tool 4 — Geospatial Query ───────────────────────────────────────────────

@tool
def geospatial_query_tool(
    ref_lat: float = 0.0,
    ref_lon: float = 0.0,
    reference_location: str | None = None,
    radius_km: float = 50.0,
    condition: str | None = None,
    analysis_type: str = "nearby",
    urban_hubs: list[str] | None = None,
) -> str:
    """
    Geospatial facility search using ST_DistanceSpheroid on the WGS84 spheroid.

    analysis_type options:
      "nearby"      — Find all facilities within radius_km.
                      Returns: list of facilities sorted by ascending distance.
      "cold_spot"   — Find regions (states) that have zero facilities matching
                      the given condition.
      "urban_rural" — Returns distance from each facility to its nearest hub.
                      If 'urban_hubs' is not provided, defaults to Ghana's 5 major.

    Args:
        ref_lat:            Latitude of reference location (optional, if known).
        ref_lon:            Longitude of reference location (optional, if known).
        reference_location: Name of the reference city/region (e.g., "Accra").
                            The tool will dynamically geocode this if provided!
        radius_km:          Search radius in kilometres (default 50).
        condition:          Optional keyword to filter by medical condition/procedure.
        analysis_type:      One of "nearby", "cold_spot", "urban_rural".
        urban_hubs:         List of city names to act as centers for urban_rural.

    Trigger keywords: "within", "km", "distance", "near", "nearby", "closest",
    "cold spot", "geographic", "radius", "proximity", "urban", "rural".
    """
    from unitycatalog.ai.langchain.toolkit import UCFunctionToolkit
    import requests
    import os

    # Dynamically geocode reference_location if ref_lat/lon not provided
    if reference_location and ref_lat == 0.0 and ref_lon == 0.0:
        api_key = os.getenv("LOCATION_IQ_ACCESS_TOKEN")
        if not api_key:
            return "[Geospatial Query Error] LOCATION_IQ_ACCESS_TOKEN not set in environment."
        try:
            resp = requests.get(
                "https://us1.locationiq.com/v1/search",
                params={"key": api_key, "q": f"{reference_location}, Ghana", "format": "json"},
                timeout=5
            )
            if resp.status_code == 200 and len(resp.json()) > 0:
                ref_lat = float(resp.json()[0]["lat"])
                ref_lon = float(resp.json()[0]["lon"])
            else:
                return f"[Geospatial Query Error] Could not dynamically geocode '{reference_location}'."
        except Exception as e:
            return f"[Geospatial Query Error] Geocoding failed: {e}"

    payload: dict = {
        "ref_lat":       ref_lat,
        "ref_lon":       ref_lon,
        "radius_km":     radius_km,
        "analysis_type": analysis_type,
    }
    if condition:
        payload["condition"] = condition

    # Dynamically geocode urban hubs if requested
    if analysis_type == "urban_rural":
        hubs_list = urban_hubs if urban_hubs else ["Accra", "Kumasi", "Tamale", "Cape Coast", "Takoradi"]
        
        api_key = os.getenv("LOCATION_IQ_ACCESS_TOKEN")
        if not api_key:
            return "[Geospatial Query Error] LOCATION_IQ_ACCESS_TOKEN not set in environment."
            
        resolved_hubs = []
        for hub in hubs_list:
            try:
                resp = requests.get(
                    "https://us1.locationiq.com/v1/search",
                    params={"key": api_key, "q": f"{hub}, Ghana", "format": "json"},
                    timeout=5
                )
                if resp.status_code == 200 and len(resp.json()) > 0:
                    data = resp.json()[0]
                    resolved_hubs.append({
                        "name": hub,
                        "lat": float(data["lat"]),
                        "lon": float(data["lon"])
                    })
            except Exception:
                pass  # Skip if unreachable
                
        if not resolved_hubs:
            return "[Geospatial Query Error] Failed to geocode urban hubs dynamically."
            
        # JSON serialize the array so the SQL parser from_json works across nested structs
        payload["urban_hubs"] = json.dumps(resolved_hubs)

    try:
        uc = UCFunctionToolkit(
            function_names=[f"{CATALOG}.{SCHEMA}.find_facilities_nearby"]
        )
        uc_fn = uc.tools[0]
        return uc_fn.invoke({"query_json": json.dumps(payload)})
    except Exception as exc:
        return f"[Geospatial Query Error] {exc}"


# ─── Tool list ────────────────────────────────────────────────────────────────

ALL_TOOLS = [genie_chat_tool, vector_search_tool, medical_agent_tool, geospatial_query_tool]


# ─── System prompt ─────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are Med-Atlas-AI, a healthcare infrastructure analyst for Ghana.

## Tool Routing — Step-by-Step Decision

Before answering, determine the query type by checking which keywords are present.
A query can match ONE or MORE types simultaneously.

### Step 1 — Classify the query (check all three):

IS_GEOSPATIAL = True if ANY of these keywords appear:
  "within", "km", "miles", "distance", "near", "nearby", "closest",
  "cold spot", "geographic", "radius", "proximity", "urban", "rural",
  "peri-urban", "how far", "geospatial", "location-based"

IS_QUANTITATIVE = True if ANY of these keywords appear:
  "how many", "count", "total", "average", "sum", "most", "least",
  "top N", "region", "district", "ownership", "beds", "capacity", "staff",
  "ratio", "percentage", "ranking", "compar", "distribution",
  "how many hospitals in [region]", "number of", "how many facilities",
  "oversupply", "scarcity", "specialist", "specialist distribution",
  "web presence", "website", "online presence",
  "doctors", "doctor count", "total doctors", "number of doctors",
  "ngo", "classification", "classify", "categorize", "breakdown",
  "affiliation", "facility type", "operator type", "organization type"

IS_SEMANTIC = True if ANY of these keywords appear:
  "similar", "like", "service", "equipment", "provides", "specialty",
  "has", "can provide", "offers", "what does", "which facilities provide",
  "capability", "capabilities", "similar to", "what services", "procedures",
  "over-claim", "implausib", "subspecialty", "equipment mismatch",
  "corrobor", "camp", "outreach", "medical camp", "referral", "bundle",
  "contradict", "inconsisten", "conflicting", "conflict"

IS_ANALYTIC = True if ANY of these keywords appear:
  "anomal", "reliab", "score", "quality",
  "gap", "unmet", "outlier", "flag",
  "duplicate", "abnormal", "red flag", "problem type", "workforce",
  "staffing", "correlat", "overlapping", "staleness", "mismatch",
  "feature mismatch", "procedure count", "equipment count", "signal"

### Step 2 — Route accordingly:

| Classification                      | Tools to Call (in order)                                    |
|-------------------------------------|-------------------------------------------------------------|
| IS_GEOSPATIAL only                  | geospatial_query_tool                                       |
| IS_GEOSPATIAL + IS_QUANTITATIVE     | geospatial_query_tool, then genie_chat_tool                 |
| IS_GEOSPATIAL + IS_SEMANTIC         | geospatial_query_tool, then vector_search_tool              |
| IS_GEOSPATIAL + IS_ANALYTIC         | geospatial_query_tool, then medical_agent_tool              |
| IS_QUANTITATIVE only                | genie_chat_tool                                             |
| IS_SEMANTIC only                    | vector_search_tool                                          |
| IS_ANALYTIC only                    | medical_agent_tool                                          |
| IS_QUANTITATIVE + IS_SEMANTIC       | genie_chat_tool, then vector_search_tool                    |
| IS_QUANTITATIVE + IS_ANALYTIC       | genie_chat_tool, then medical_agent_tool                    |
| IS_SEMANTIC + IS_ANALYTIC           | vector_search_tool, then medical_agent_tool                 |
| ALL THREE (no geo)                  | genie_chat_tool → vector_search_tool → medical_agent_tool   |

### Step 2.5 — Geospatial Protocol (applies when IS_GEOSPATIAL = True):

If the user asks for a physical distance search (e.g., "within 50 km of Accra"), you do NOT need to look up exact coordinates.
Simply pass `reference_location="Accra"` to the `geospatial_query_tool`, and it will dynamically fetch the latitude/longitude for you!

**Choosing `analysis_type`:**
  • "nearby"      — user asks "within X km" or "near" a location. Provide `reference_location` and `radius_km`.
  • "cold_spot"   — user asks about regions lacking a service, geographic gaps.
  • "urban_rural" — user asks about urban vs rural service distribution. You may optionally pass a list of `urban_hubs` 
                    (e.g., `urban_hubs=["Accra", "Kumasi"]`) to dynamically define the urban centers for this search.

### Step 2.5 — Medical Reasoning Protocol (applies when query involves medical domain judgment):

If the query involves ANY of:
  • over-claiming or implausible services (e.g., "clinic doing brain surgery")
  • equipment-capability mismatches (e.g., "has MRI but no radiologist")
  • subspecialty vs infrastructure plausibility (e.g., "neurosurgery at a small clinic")
  • general capability plausibility (e.g., "can this facility actually do this?")

Then follow this 3-step reasoning protocol:
  1. Use genie_chat_tool to fetch the raw facility profile:
     → Ask for: facility_name, facility_type, specialties, procedures, equipment, capacity, no_doctors, social_links
     → Filter to the relevant facilities (e.g., clinics, pharmacies, dentists)
  2. Use vector_search_tool with fact_type=["specialty", "equipment", "procedure", "summary", "capability"] to retrieve the detailed fact_text for those facilities.
  3. Apply YOUR OWN medical expertise:
     → Is this facility_type capable of these procedures given real-world medical standards?
     → Does this equipment require specialist support that is not present?
     → Is this subspecialty realistic given the facility's size and capacity?
     → DO NOT use simple keyword matching — reason about plausibility holistically.
  4. Report findings with: facility name, facility type, the suspicious claim, your
     medical reasoning, and severity (high/medium/low).

### Step 2.5 — Contradiction Detection Protocol (applies when query involves contradictions or inconsistencies):

If the user asks about **conflicting claims**, **contradictory information**, or **inconsistent data** for a facility:
  1. Use `vector_search_tool` with the topic they mention (e.g., `query="ICU surgery contradictions"`) to semantically retrieve all related facts from `facility_facts`
  2. **Group the results** by `facility_id` (available in each Document's metadata)
  3. For any facility with **2 or more facts** on the same topic, compare the claims:
     → Does Fact A say it has a capability that Fact B denies?
     → Are there mutually exclusive claims (e.g., "no surgical unit" vs "performs cardiac surgery")?
  4. Report findings with: facility name, the conflicting fact excerpts, your medical reasoning, and severity (high/medium/low)
  5. If no conflicts are found, say so clearly.

### Step 2.5 — Reliability Scoring Protocol (applies when medical_agent_tool returns `reliability_score` findings):

When `medical_agent_tool` returns `type: reliability_score` data, present each facility as a **friendly, conversational summary card**. Your goal is to make a non-technical user understand how trustworthy the information is.

**Format per facility:**
```
### 🏥 [Facility Name] — [facility_type]
**Overall Data Confidence:** [reliability_score]/100 ([HIGH / MEDIUM / LOW])

[If deduction_reasons is NOT empty:]
Here is what we could not fully confirm about this facility:
• [deduction_reason 1 — use as-is, it is already in plain language]
• [deduction_reason 2 ...]

[If deduction_reasons IS empty OR data_gaps is 0:]
✅ This facility has a well-documented profile — all key information is available.
```

**Strict language rules:**
  • NEVER mention: NULL, fields, penalties, scores, databases, SQL, or any technical term
  • NEVER say "bed capacity field is null" — say "we don't know how many beds this facility has"
  • NEVER present missing data as zero — say "this information is not available"
  • Use the `deduction_reasons` list exactly as written — they are already user-friendly
  • If `data_gaps > 3`, add a note: *"This profile is incomplete — treat its information with caution"*
  • If `data_gaps = 0`, affirm that the profile is complete and trustworthy

**After all facility cards:** Write a 2-sentence plain-English summary of overall data quality across all facilities (e.g., "Most facilities have partial profiles. Key gaps are around staffing numbers and medical equipment records.").

### Step 2.5 — Anomaly Classification Protocol (applies after calling medical_agent_tool):

When `medical_agent_tool` returns raw structural data, you MUST classify it based on its `type`:
  • For `anomaly_flagging` (Outlier Detection):
      1. **ALWAYS start** by reading `data_coverage_summary`. Before listing any outliers, tell the user honestly how much data was available. Example: *"Please note: bed count information is only available for 18% of facilities in our dataset — the remaining 82% could not be assessed for this check."*
      2. For each flagged facility, present the `reason` field directly — it is already written in plain language. Do NOT add statistical jargon (no "standard deviations", no "sigma", no "mean ± std").
      3. If `findings` is empty (`[]`), tell the user: *"No unusual values were found among the facilities where bed and doctor data is available. However, this could not be checked for the majority of facilities due to missing data."*
      4. NEVER present the raw numbers as proof of wrongdoing — frame it as *"this may need verification"* not *"this is wrong"*.
  • For `regional_coverage` (Unmet Needs):
      - `specialties_missing` is a **pre-computed, definitive SQL list** — report every specialty in it as a **confirmed gap** for that region (these exist elsewhere in the dataset but not here).
      - For `procedures_present` and `equipment_present` (free-text): apply your medical domain knowledge to identify what services or equipment a region of that size and facility count would typically need but appears to lack. These are NOT pre-computed gaps — they require your reasoning.
  • For `feature_mismatch_raw` (Procedure/Equipment Mismatch):
      1. **ALWAYS start** by reading `data_coverage_summary`. Tell the user how many facilities are missing equipment data before listing any findings.
      2. Group your findings by `flag_type`:
         - **`missing_equipment`:** Explain these are *unverifiable* (they claim procedures but equipment data is missing). Do NOT call these anomalies or overclaims.
         - **`implausible_ratio`:** Use your medical expertise to evaluate if the ratio of procedures to equipment is medically implausible for the `facility_type`. (e.g., A clinic claiming 15 procedures with 1 piece of equipment may be an overclaim).
  • For `facility_profile_counts` (Problem Type):
      1. **ALWAYS start** by using `data_coverage_summary` to state the systemic data availability (e.g. "We only have equipment data for X% of facilities").
      2. For each facility, check the `_status` fields (`equipment_status`, `specialty_status`, `procedure_status`):
         - If a status is **`missing_data`**: The database simply lacks records for this category. Do NOT diagnose this as a medical gap. Group these as "Facilities with Unverifiable Missing Data" and explain why they cannot be fully evaluated.
         - If a status is **`true_zero`**: This is a confirmed absence of capability. Classify these true medical gaps as "equipment_gap" (has doctors/specialties but truly 0 equipment), "service_gap" (has equipment but truly 0 services/procedures), or "overclaim_gap" (claims many procedures but has 0 verifiable specialties).
  • For `ngo_overlap_raw`: Evaluate if multiple facilities with the exact same NGO affiliation in the same city represent a duplication of services or complementary care.

### Step 3 — Multi-tool orchestration:

After receiving each tool result, decide:
  • If more data is needed from another tool → call the next tool in sequence
  • If all necessary data is collected → synthesize a comprehensive markdown answer
  • If a tool returns an error or empty results → try the next appropriate tool as fallback
  • Never repeat the same tool twice for the same purpose

### Step 4 — Response format:

• You MUST ALWAYS provide a final, human-readable response in Markdown format after your tool calls are complete.
• NEVER respond with raw JSON, raw tool outputs, or unformatted text as your final answer.
• If you called multiple tools, synthesize their results together into a single cohesive summary.
• Cite specific facility names and regions.
• Format tabular results as markdown tables.
• If no results are found, say so clearly and suggest trying a different approach.
"""


# ─── LangGraph ────────────────────────────────────────────────────────────────

class AgentState(TypedDict):
    messages: Annotated[Sequence, add_messages]


llm = ChatDatabricks(endpoint=LLM_ENDPOINT, temperature=0.1, max_tokens=2048)
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


def build_graph():
    graph = StateGraph(AgentState)
    graph.add_node("agent", RunnableLambda(call_model))
    graph.add_node("tools", ToolNode(ALL_TOOLS))
    graph.add_conditional_edges("agent", should_continue, {"tools": "tools", "end": END})
    graph.add_edge("tools", "agent")
    graph.set_entry_point("agent")
    return graph.compile()


# ─── Stream aggregator ─────────────────────────────────────────────────────────
#
# output_to_responses_items_stream() processes each message independently and
# emits function_call / function_call_output as separate events. For a
# readable tool-call sequence, we aggregate them and emit:
#
#   function_call → function_call_output → message
#
# in execution order. This lets API consumers see exactly which tools ran and
# in what sequence before the final answer.

class _ToolCallTracker:
    """Collects tool calls and tool results from LangGraph message stream.

    Also builds a structured citation registry from tool outputs.
    Each tool's output is parsed to extract source rows from:
      - facility_facts  (vector_search_tool)
      - facility_records (medical_agent_tool, genie_chat_tool, geospatial_query_tool)
      - regional_insights (medical_agent_tool Unmet Needs, genie_chat_tool)
    """

    def __init__(self):
        # List of {call_id, name, arguments} seen so far
        self.pending_calls: list[dict[str, Any]] = []
        # call_id → result string
        self.call_results: dict[str, str] = {}
        # List of stream events to yield in order
        self.events: list[ResponsesAgentStreamEvent] = []
        self.output_index = 0
        # Citation registry: ordered list of step citations
        self._citations: list[dict[str, Any]] = []
        # step index counter (increments per tool call)
        self._step_index = 0

    # ── Citation parsers ──────────────────────────────────────────────────────

    @staticmethod
    def _parse_vector_search_citations(call_id: str, call_name: str,
                                       call_args: dict, raw_output: str) -> dict[str, Any]:
        """
        Parse vector_search_tool output.
        Output is a list of LangChain Documents with metadata from facility_facts.
        """
        sources: list[dict[str, Any]] = []
        try:
            # The raw output from vector_search_tool is a Python repr of a list of Documents.
            # We extract structured data using regex on the metadata dict.
            import re
            # Match each document block
            doc_pattern = re.compile(
                r"metadata=\{([^}]+)\}.*?page_content='(.*?)(?='\))",
                re.DOTALL,
            )
            for m in doc_pattern.finditer(raw_output):
                meta_str = m.group(1)
                page_content = m.group(2).strip()
                meta: dict[str, str] = {}
                for kv in re.finditer(r"'(\w+)':\s*'([^']*)'" , meta_str):
                    meta[kv.group(1)] = kv.group(2)
                # Attempt to get facility name from page_content prefix
                name_match = re.match(r'^([^\n]+?) in ', page_content)
                facility_name = name_match.group(1).strip() if name_match else None
                snippet = page_content[:200] + "..." if len(page_content) > 200 else page_content
                sources.append({
                    "source_type": "facility_facts",
                    "fact_id": meta.get("fact_id"),
                    "facility_id": meta.get("facility_id"),
                    "facility_name": facility_name,
                    "fact_type": meta.get("fact_type"),
                    "excerpt": snippet,
                })
        except Exception:
            pass
        return {
            "step_index": None,  # filled in by caller
            "tool_name": call_name,
            "call_id": call_id,
            "query_used": call_args.get("query", ""),
            "tables_accessed": ["facility_facts"],
            "sources": sources,
        }

    @staticmethod
    def _parse_medical_agent_citations(call_id: str, call_name: str,
                                       call_args: dict, raw_output: str) -> dict[str, Any]:
        """
        Parse medical_agent_tool output.
        Returns JSON with a 'findings' array from facility_records and/or facility_facts.
        """
        sources: list[dict[str, Any]] = []
        tables_accessed: set[str] = {"facility_records"}
        try:
            data = json.loads(raw_output)
            findings = data.get("findings") or []
            if isinstance(findings, str):
                findings = json.loads(findings)
            for f in (findings or []):
                if not isinstance(f, dict):
                    continue
                finding_type = f.get("type", "")
                # Determine which tables this finding draws from
                if finding_type in ("contradictory_signals", "feature_mismatch_raw",
                                    "ngo_raw_data", "ngo_overlap_raw"):
                    tables_accessed.add("facility_facts")
                if finding_type in ("regional_coverage",):
                    tables_accessed.add("regional_insights")
                # Build a source entry
                source: dict[str, Any] = {
                    "source_type": "facility_records",
                    "finding_type": finding_type,
                    "facility_id": f.get("facility_id") or f.get("region"),
                    "facility_name": f.get("facility_name") or f.get("region"),
                    "severity": f.get("severity"),
                    "note": f.get("note") or f.get("reason") or f.get("recommendation"),
                }
                # For regional findings, override source_type
                if finding_type in ("regional_coverage", "data_staleness"):
                    source["source_type"] = "facility_records"
                if finding_type == "regional_coverage":
                    source["source_type"] = "regional_insights"
                    source["region"] = f.get("region")
                    source["total_facilities"] = f.get("total_facilities")
                sources.append(source)
        except Exception:
            pass
        return {
            "step_index": None,
            "tool_name": call_name,
            "call_id": call_id,
            "query_used": call_args.get("query", ""),
            "tables_accessed": sorted(tables_accessed),
            "sources": sources,
        }

    @staticmethod
    def _parse_genie_citations(call_id: str, call_name: str,
                               call_args: dict, raw_output: str) -> dict[str, Any]:
        """
        Parse genie_chat_tool output.
        Genie returns free text but often mentions facility names or regions.
        We record the query and try to detect which tables are referenced.
        """
        import re
        tables_accessed: list[str] = []
        raw_lower = raw_output.lower()
        if any(w in raw_lower for w in ["facility", "hospital", "clinic", "operator"]):
            tables_accessed.append("facility_records")
        if any(w in raw_lower for w in ["region", "state", "district", "insight"]):
            tables_accessed.append("regional_insights")
        if not tables_accessed:
            tables_accessed = ["facility_records"]
        # Try to capture a short snippet of the genie response
        snippet = raw_output[:400] + "..." if len(raw_output) > 400 else raw_output
        return {
            "step_index": None,
            "tool_name": call_name,
            "call_id": call_id,
            "query_used": call_args.get("query", ""),
            "tables_accessed": tables_accessed,
            "sources": [{
                "source_type": "genie_response",
                "tables_queried": tables_accessed,
                "excerpt": snippet,
            }],
        }

    @staticmethod
    def _parse_geospatial_citations(call_id: str, call_name: str,
                                    call_args: dict, raw_output: str) -> dict[str, Any]:
        """
        Parse geospatial_query_tool output.
        Returns a JSON array of facility records with distances.
        """
        sources: list[dict[str, Any]] = []
        try:
            data = json.loads(raw_output)
            results = data if isinstance(data, list) else data.get("facilities", [])
            for r in (results or []):
                if not isinstance(r, dict):
                    continue
                sources.append({
                    "source_type": "facility_records",
                    "facility_id": r.get("facility_id"),
                    "facility_name": r.get("facility_name"),
                    "city": r.get("city"),
                    "state": r.get("state"),
                    "distance_km": r.get("distance_km"),
                })
        except Exception:
            pass
        return {
            "step_index": None,
            "tool_name": call_name,
            "call_id": call_id,
            "query_used": str(call_args),
            "tables_accessed": ["facility_records"],
            "sources": sources,
        }

    def _extract_citations(self, call_id: str, tool_content: str) -> None:
        """Look up the matching tool call and dispatch to the right parser."""
        # Find the tool call details from pending_calls
        call_info = next(
            (c for c in self.pending_calls if c["call_id"] == call_id),
            None,
        )
        if not call_info:
            return
        name = call_info["name"]
        args = call_info["arguments"] if isinstance(call_info["arguments"], dict) else {}

        if name == "vector_search_tool":
            citation = self._parse_vector_search_citations(call_id, name, args, tool_content)
        elif name == "medical_agent_tool":
            citation = self._parse_medical_agent_citations(call_id, name, args, tool_content)
        elif name == "genie_chat_tool":
            citation = self._parse_genie_citations(call_id, name, args, tool_content)
        elif name == "geospatial_query_tool":
            citation = self._parse_geospatial_citations(call_id, name, args, tool_content)
        else:
            citation = {
                "step_index": None,
                "tool_name": name,
                "call_id": call_id,
                "query_used": str(args),
                "tables_accessed": [],
                "sources": [],
            }

        citation["step_index"] = self._step_index
        self._step_index += 1
        self._citations.append(citation)

    def get_citations(self) -> dict[str, Any]:
        """Return the full citation object for inclusion in the API response."""
        all_facilities: list[str] = []
        all_tools: list[str] = []
        all_tables: list[str] = []
        total_sources = 0

        for step in self._citations:
            tool = step.get("tool_name", "")
            if tool and tool not in all_tools:
                all_tools.append(tool)
            for tbl in step.get("tables_accessed", []):
                if tbl not in all_tables:
                    all_tables.append(tbl)
            for src in step.get("sources", []):
                total_sources += 1
                name = src.get("facility_name")
                if name and name not in all_facilities:
                    all_facilities.append(name)

        return {
            "steps": self._citations,
            "summary": {
                "total_sources": total_sources,
                "facilities_referenced": all_facilities,
                "tools_used": all_tools,
                "tables_accessed": all_tables,
            },
        }

    # ── Event helpers ─────────────────────────────────────────────────────────

    def _emit(self, item: OutputItem) -> None:
        self.events.append(
            ResponseOutputItemDoneEvent(
                item=item,
                output_index=self.output_index,
                type="response.output_item.done",
            )
        )
        self.output_index += 1

    def process_message(self, msg: Any) -> None:
        """
        Process a single LangChain message and update the tracker.
        Emits events immediately for tool_calls (function_call) and tool (function_call_output).
        Accumulates text for the final message.
        """
        msg_type = getattr(msg, "type", None)
        msg_id = getattr(msg, "id", None) or str(uuid.uuid4())

        if msg_type == "ai":
            content = getattr(msg, "content", None) or ""

            # 1) Emit function_call items first
            tool_calls = getattr(msg, "tool_calls", None) or []
            for tc in tool_calls:
                call_id = tc.get("id") or str(uuid.uuid4())
                tc_name = tc.get("name", "unknown")
                tc_args = tc.get("args", {})
                if isinstance(tc_args, str):
                    try:
                        tc_args = json.loads(tc_args)
                    except Exception:
                        pass

                self.pending_calls.append({
                    "call_id": call_id,
                    "name": tc_name,
                    "arguments": tc_args,
                })

                self._emit(OutputItem(
                    type="function_call",
                    id=msg_id,
                    name=tc_name,
                    call_id=call_id,
                    arguments=json.dumps(tc_args, indent=2),
                ))

            # 2) Emit text content as message (may be empty if this is a tool-call-only turn)
            if content.strip():
                self._emit(OutputItem(
                    type="message",
                    id=msg_id,
                    role="assistant",
                    content=[Content(type="output_text", text=content)],
                ))

        elif msg_type == "tool":
            # Tool result — store it and emit function_call_output immediately after
            # the corresponding function_call. We emit it in the order it arrives.
            call_id = getattr(msg, "tool_call_id", None) or "unknown"
            tool_content = getattr(msg, "content", None) or ""
            self.call_results[call_id] = tool_content

            # Extract citations from this tool result
            self._extract_citations(call_id, tool_content if isinstance(tool_content, str) else str(tool_content))

            # Emit the function_call_output event
            self._emit(OutputItem(
                type="function_call_output",
                call_id=call_id,
                output=tool_content,
            ))

        elif msg_type in ("user", "human"):
            # Skip user messages in output
            pass

    def finalize(self) -> list[ResponsesAgentStreamEvent]:
        """Return all collected events in order."""
        return self.events



# ─── ResponsesAgent (MLflow 3.x deployment interface) ──────────────────────────

from typing import NamedTuple


class _AgentResult(NamedTuple):
    """Internal result wrapper carrying both the response and citation data."""
    response: ResponsesAgentResponse
    citations: dict[str, Any]


class MedAtlasAgent(ResponsesAgent):
    def __init__(self):
        self.graph = build_graph()

    def _run_graph(
        self, request: ResponsesAgentRequest, tracker: _ToolCallTracker
    ) -> list[ResponsesAgentStreamEvent]:
        """Run the LangGraph, process all messages into tracker, return events."""
        messages = to_chat_completions_input([m.model_dump() for m in request.input])
        for event in self.graph.stream(
            {"messages": messages},
            config={"recursion_limit": 100},
            stream_mode=["updates"],
        ):
            if event[0] != "updates":
                continue
            for node_data in event[1].values():
                if not isinstance(node_data, dict) or not node_data.get("messages"):
                    continue
                for msg in node_data["messages"]:
                    tracker.process_message(msg)
        return tracker.finalize()

    def predict(self, request: ResponsesAgentRequest) -> ResponsesAgentResponse:
        """MLflow-compatible predict (no citations)."""
        tracker = _ToolCallTracker()
        events = self._run_graph(request, tracker)
        outputs = [e.item for e in events if e.type == "response.output_item.done"]
        return ResponsesAgentResponse(output=outputs)

    def predict_with_citations(self, request: ResponsesAgentRequest) -> _AgentResult:
        """Run the agent and return both the response AND structured citations."""
        tracker = _ToolCallTracker()
        events = self._run_graph(request, tracker)
        outputs = [e.item for e in events if e.type == "response.output_item.done"]
        return _AgentResult(
            response=ResponsesAgentResponse(output=outputs),
            citations=tracker.get_citations(),
        )

    def predict_stream(
        self, request: ResponsesAgentRequest
    ) -> Generator[ResponsesAgentStreamEvent, None, None]:
        """Streaming predict — yields events as they arrive (no citations in stream)."""
        tracker = _ToolCallTracker()
        yield from self._run_graph(request, tracker)


# ─── Export ────────────────────────────────────────────────────────────────────

AGENT = MedAtlasAgent()
mlflow.models.set_model(AGENT)

