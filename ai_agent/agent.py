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
    "least", "top N", "region", "district", "state", "ownership", "beds",
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

    Detects 15 anomaly types (statistical/mathematical — NOT medical domain reasoning):
      1. contradictory_signals    — conflicting ICU/inpatient statements across facts
      2. ngo_classification       — direct_operator / supporter / none
      3. reliability_score        — 0-100 data quality score based on fact density
      4. unmet_needs              — service gaps by region
      5. duplicate_facility       — same name prefix in multiple records
      6. anomaly_flagging         — outlier bed/doctor counts (3 std devs)
      7. abnormal_ratio           — implausible bed/OR/doctor ratios
      8. feature_mismatch         — procedure COUNT vs equipment COUNT (numeric)
      9. oversupply_scarcity      — procedure frequency vs facility count
     10. ngo_overlap              — overlapping NGO presence in same region
     11. problem_type             — classify gaps as equipment/training/workforce
     12. specialist_distribution  — specialty → region mapping
     13. web_capability_mismatch  — description length vs fact count discrepancy
     14. visiting_vs_permanent    — facility_type vs doctor count patterns
     15. data_staleness           — outdated records (updated_at age scoring)

    NOTE: Over-claiming, equipment-procedure mismatch, and subspecialty-infrastructure
    mismatch are NOT handled here — those require medical domain reasoning and are
    handled directly by the LLM using genie_chat_tool + vector_search_tool.

    Trigger keywords: "anomal", "inconsisten", "contradict", "reliab", "score",
    "quality", "ngo", "classify", "gap", "unmet", "outlier", "flag",
    "duplicate", "abnormal", "red flag", "problem type", "workforce",
    "specialist", "visiting staff", "permanent staff", "oversupply",
    "scarcity", "overlapping", "staleness", "stale", "corrobor".

    Args:
        query:      Analysis question (e.g., "detect anomalies", "score reliability")
        facility_id: Optional. Restrict analysis to one facility.

    Returns: Structured JSON with findings, severity, and recommendations.
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
  "top N", "region", "district", "ownership", "beds", "staff",
  "ratio", "percentage", "ranking", "compar", "distribution",
  "how many hospitals in [region]", "number of", "how many facilities"

IS_SEMANTIC = True if ANY of these keywords appear:
  "similar", "like", "service", "equipment", "provides", "specialty",
  "has", "can provide", "offers", "what does", "which facilities provide",
  "capability", "capabilities", "similar to", "what services", "procedures",
  "over-claim", "implausib", "subspecialty", "equipment mismatch",
  "corrobor", "camp", "outreach", "medical camp", "referral", "bundle"

IS_ANALYTIC = True if ANY of these keywords appear:
  "anomal", "inconsisten", "contradict", "reliab", "score", "quality",
  "ngo", "classify", "classif", "gap", "unmet", "outlier", "flag",
  "duplicate", "abnormal", "red flag", "problem type", "workforce",
  "specialist", "specialist workforce", "visiting staff", "permanent staff",
  "oversupply", "scarcity", "correlat", "overlapping", "staleness"

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
     → Ask for: facility_name, facility_type, specialties, procedures, equipment, capacity, number_doctors
     → Filter to the relevant facilities (e.g., clinics, pharmacies, dentists)
  2. Use vector_search_tool with fact_type=["specialty", "equipment", "procedure", "summary", "capability"] to retrieve the detailed fact_text for those facilities.
  3. Apply YOUR OWN medical expertise:
     → Is this facility_type capable of these procedures given real-world medical standards?
     → Does this equipment require specialist support that is not present?
     → Is this subspecialty realistic given the facility's size/capacity/doctor count?
     → DO NOT use simple keyword matching — reason about plausibility holistically.
  4. Report findings with: facility name, facility type, the suspicious claim, your
     medical reasoning, and severity (high/medium/low).

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
    """Collects tool calls and tool results from LangGraph message stream."""

    def __init__(self):
        # List of {call_id, name, arguments} seen so far
        self.pending_calls: list[dict[str, Any]] = []
        # call_id → result string
        self.call_results: dict[str, str] = {}
        # List of stream events to yield in order
        self.events: list[ResponsesAgentStreamEvent] = []
        self.output_index = 0

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

class MedAtlasAgent(ResponsesAgent):
    def __init__(self):
        self.graph = build_graph()

    def predict(self, request: ResponsesAgentRequest) -> ResponsesAgentResponse:
        outputs = [
            event.item
            for event in self.predict_stream(request)
            if event.type == "response.output_item.done"
        ]
        return ResponsesAgentResponse(output=outputs)

    def predict_stream(
        self, request: ResponsesAgentRequest
    ) -> Generator[ResponsesAgentStreamEvent, None, None]:
        # Convert input to LangGraph message format
        messages = to_chat_completions_input([m.model_dump() for m in request.input])

        tracker = _ToolCallTracker()

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

        yield from tracker.finalize()


# ─── Export ────────────────────────────────────────────────────────────────────

AGENT = MedAtlasAgent()
mlflow.models.set_model(AGENT)
