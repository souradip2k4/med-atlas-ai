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
import re
import uuid
import warnings
import mlflow
import os
from pathlib import Path
from typing import Annotated, Any, Generator, Sequence, TypedDict

from dotenv import load_dotenv
_env_path = Path(__file__).parent.parent / ".env"
load_dotenv(_env_path)

experiment_id = os.getenv("MLFLOW_EXPERIMENT_ID")
tracking_uri = os.getenv("MLFLOW_TRACKING_URI")
registry_uri = os.getenv("MLFLOW_REGISTRY_URI")
# Optional overrides:
# - In Databricks Apps, experiment resource injection is enough for tracking.
# - For local remote tracking, you can still set MLFLOW_TRACKING_URI / auth env vars.
if tracking_uri:
    mlflow.set_tracking_uri(tracking_uri)

# Registry URI selection:
# - Honor explicit MLFLOW_REGISTRY_URI.
# - Default to Unity Catalog registry when using Databricks tracking.
if registry_uri:
    mlflow.set_registry_uri(registry_uri)

try:
    if experiment_id:
        mlflow.set_experiment(experiment_id=experiment_id)
    else:
        warnings.warn(
            "No MLflow experiment configured. Set MLFLOW_EXPERIMENT_ID (recommended in Databricks Apps)."
        )
except mlflow.exceptions.MlflowException as exc:
    warnings.warn(
        f"MLflow experiment setup failed ({exc}). Continuing without forcing experiment selection."
    )
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
VS_INDEX     = os.environ.get("VECTOR_SEARCH_INDEX")
GENIE_ID     = os.environ["GENIE_SPACE_ID"]
CATALOG      = os.environ.get("CATALOG")
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


# ─── Tool 2 — Vector Search ───────────────────────────────────────────────────

@tool
def vector_search_tool(query: str, fact_types: list[str] | str | None = None) -> str:
    """
    Semantic search over pre-generated facility facts stored in the facility_facts table.

    Best for: "Which facilities provide cardiac surgery?", "has MRI?",
    "similar to [name]", specialized services, capabilities, equipment.

    Args:
        query:      Natural language search query
        fact_types: Optional filter to specific fact types.
                    Valid values and what each type contains:
                      - "specialty"   : Medical specialty tags a facility offers
                                        (e.g., internalMedicine, dentistry, gynecologyAndObstetrics)
                      - "procedure"   : Specific medical procedures performed in plain text
                                        (e.g., "Offers teeth whitening", "fertility management")
                      - "equipment"   : Physical devices/machines on-site
                                        (e.g., "Automatic changeover oxygen manifold", "operating room equipment")
                      - "capability"  : Operational context — hours, departments, contact info,
                                        accreditations, social media, 24/7 availability
                      - "summary"     : One-line facility profile: type, location, affiliation,
                                        general description
                    Pass a list like ["procedure", "equipment"] or a single string like "specialty".
                    If None, searches across all fact types (use only when cross-type context is needed).
    """
    from databricks_langchain import VectorSearchRetrieverTool

    if isinstance(fact_types, str):
        fact_types = [fact_types]

    kwargs = {
        "index_name": VS_INDEX,
        "num_results": 45,
        "columns": ["fact_id", "facility_id", "fact_text", "fact_type"],
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

# Batch size for deep validation LLM calls
_DEEP_VALIDATION_BATCH_SIZE = 15

_DEEP_VALIDATION_PROMPT = """You are a medical infrastructure validator. Analyze each facility below for specialty↔procedure↔equipment consistency.

For EACH facility, check:
1. SPECIALTY→PROCEDURE: Do the procedures match the claimed specialties?
   Example mismatch: "Cardiology" specialty + "Appendectomy" procedure
2. PROCEDURE→EQUIPMENT: Can these procedures be performed with this equipment?
   Example mismatch: "MRI Scan" procedure + no MRI machine in equipment
3. SPECIALTY→EQUIPMENT: Does the equipment support the claimed specialty?
   Example mismatch: "Ophthalmology" specialty + only "Stethoscope" equipment
4. FACILITY_TYPE plausibility: Can this facility type realistically support these subspecialties?
   Example mismatch: "clinic" + "Neurosurgery" specialty
5. CAPACITY check: If capacity/no_doctors is available, is it realistic for the claimed services?

For each facility WITH ANOMALIES, write a compact 2-3 line blurb:
[SEVERITY: high|medium|low] [Facility Name] – [1-sentence description of the core mismatch]. Missing: [list key missing equipment or inconsistencies].
[Reasoning] - [brief medical reasoning]
Rules:
- ONLY write blurbs for facilities with clear anomalies. Skip consistent ones entirely.
- If a facility has completeness != "full", note missing data and that validation is limited.
- If ALL facilities in this batch are consistent, write only: NO_ANOMALIES_IN_BATCH
- DO NOT return JSON. Write plain text blurbs only.

Facilities to analyze:
"""




@tool
def medical_agent_tool(
    query: str,
    facility_name: str | None = None,
    facility_id: str | None = None,
    facility_ids: list[str] | None = None,
    region: str | None = None,
    city: str | None = None,
    operator_type: str | None = None,
    organization_type: str | None = None,
    facility_type: str | None = None,
    affiliation_type: str | None = None,
) -> str:
    """
    Medical domain reasoning and anomaly detection on facility data.

    Uses the analyze_medical_query UC function on facility_records
    to detect data quality issues and anomalies.

    Returns data for 5 analysis types:
      1. regional_coverage        — per-region service coverage arrays for LLM gap analysis
      2. anomaly_flagging         — outlier capacity/doctor counts (3 std devs, global baseline)
      3. ngo_overlap_raw          — NGOs grouped by affiliation+region for LLM overlap analysis
      4. facility_profile_counts  — raw per-facility counts for LLM gap classification
      5. deep_validation          — region-scoped specialty↔procedure↔equipment consistency check
                                    (also handles feature/equipment mismatch queries)
                                    (batched internally, 8 facilities per LLM call)
                                    REQUIRES: region OR facility_name OR facility_id

    NOTE — For classification/breakdown queries use genie_chat_tool instead.
    NOTE — For contradiction detection, use vector_search_tool instead.

    IMPORTANT — For branches that return raw data (types 1, 3, 6), YOU must synthesize
      a meaningful analysis — do NOT just echo the raw data.

    SCOPE FILTERS (all optional — apply ONLY what the user explicitly mentioned):
        facility_ids:      List of facility IDs (e.g., from geospatial_query_tool output).
                           Pass this when the user asks for anomaly/validation analysis
                           on a set of facilities returned by a radius/geospatial search.
        operator_type:     'private' | 'public'
        organization_type: 'facility' | 'ngo'
        facility_type:     'hospital' | 'clinic' | 'dentist' | 'farmacy' | 'doctor'
        affiliation_type:  'faith-tradition' | 'government' | 'community' |
                           'philanthropy-legacy' | 'academic'
        region:            Exact region/state name (e.g., 'Northern', 'Greater Accra').
                           Required for deep_validation.
        city:              City name (optional, narrows the scope within the region).
        facility_id:       Restrict analysis to a single facility by its UUID.
        facility_name:     Partial name match (e.g., 'Korle-Bu').

    Trigger keywords: "anomal", "ngo", "classify", "gap", "unmet",
    "outlier", "flag", "abnormal", "red flag",
    "problem type", "workforce", "staffing", "overlapping", "corrobor",
    "mismatch", "feature mismatch", "procedure count", "equipment count",
    "validate", "consistency", "verify claim", "capable", "infrastructure".

    NOT for: oversupply, scarcity, specialist distribution, web presence → use genie_chat_tool.

    Returns: Structured JSON with findings + optional 'note' fields for LLM reasoning.
    """
    import math
    from unitycatalog.ai.langchain.toolkit import UCFunctionToolkit

    args = {"query": query}
    if facility_name:
        args["facility_name"] = facility_name
    if facility_id:
        args["facility_id"] = facility_id
    if facility_ids:
        args["facility_ids"] = json.dumps(facility_ids)
    if region:
        args["region"] = region
    if city:
        args["city"] = city
    if operator_type:
        args["operator_type"] = operator_type
    if organization_type:
        args["organization_type"] = organization_type
    if facility_type:
        args["facility_type"] = facility_type
    if affiliation_type:
        args["affiliation_type"] = affiliation_type

    try:
        uc = UCFunctionToolkit(
            function_names=[f"{CATALOG}.{SCHEMA}.analyze_medical_query"]
        )
        uc_fn = uc.tools[0]
        raw_result = uc_fn.invoke({"query_json": json.dumps(args)})
        outer = json.loads(raw_result)

        # UCFunctionToolkit wraps every UC function return in
        #   {"format": "SCALAR", "value": "<json-string>"}.
        # Unwrap to get the actual SQL return value before reading any keys.
        if isinstance(outer, dict) and "value" in outer and "format" in outer:
            inner = outer["value"]
            outer = json.loads(inner) if isinstance(inner, str) else inner

        # Check for error responses (e.g., missing region for deep validation)
        if "error" in outer:
            return json.dumps(outer, indent=2)

        findings_raw = outer.get("findings", "[]")
        findings = json.loads(findings_raw) if isinstance(findings_raw, str) else findings_raw
        outer["findings"] = findings

        # Also parse data_coverage_summary (Branches 3, 4, 6 return it as a JSON string)
        cov_raw = outer.get("data_coverage_summary")
        if isinstance(cov_raw, str):
            try:
                outer["data_coverage_summary"] = json.loads(cov_raw)
            except (json.JSONDecodeError, TypeError):
                pass  # keep as-is if not valid JSON


        # ── Batched LLM Evaluation ────────────────────────────────────────
        # Only deep_validation requires Python-level batching.
        # Feature mismatch is now fully handled inside Branch 7's deep_validation path.
        _LLM_BATCH_TYPES = {
            "deep_validation": _DEEP_VALIDATION_PROMPT,
        }
        finding_type_0 = findings[0].get("type") if (
            findings and isinstance(findings, list) and len(findings) > 0
            and isinstance(findings[0], dict)
        ) else None

        if finding_type_0 in _LLM_BATCH_TYPES:
            batch_prompt_template = _LLM_BATCH_TYPES[finding_type_0]
            batch_llm = ChatDatabricks(
                endpoint=LLM_ENDPOINT, temperature=0.0, max_tokens=4096
            )
            validation_text_lines: list[str] = []
            total_batches = math.ceil(len(findings) / _DEEP_VALIDATION_BATCH_SIZE)

            for i in range(0, len(findings), _DEEP_VALIDATION_BATCH_SIZE):
                batch = findings[i:i + _DEEP_VALIDATION_BATCH_SIZE]
                batch_num = (i // _DEEP_VALIDATION_BATCH_SIZE) + 1
                batch_prompt = batch_prompt_template + json.dumps(batch, indent=2)

                try:
                    with mlflow.start_span(
                        name=f"deep_validation_batch_{batch_num}/{total_batches}"
                    ) as span:
                        span.set_attribute("batch_num", batch_num)
                        span.set_attribute("total_batches", total_batches)
                        span.set_attribute("batch_size", len(batch))
                        span.set_attribute(
                            "facilities",
                            [f.get("facility_name", "unknown") for f in batch]
                        )

                        response = batch_llm.invoke([HumanMessage(content=batch_prompt)])
                        response_text = response.content
                        # Reasoning models return a list of content blocks — extract only text blocks.
                        if isinstance(response_text, list):
                            response_text = "\n".join(
                                block.get("text", "") if isinstance(block, dict) else str(block)
                                for block in response_text
                                if not (isinstance(block, dict) and block.get("type") == "reasoning")
                            )
                        response_text = response_text.strip()

                        has_anomalies = bool(response_text and response_text != "NO_ANOMALIES_IN_BATCH")
                        span.set_attribute("anomalies_found", has_anomalies)

                        # Append non-empty, non-trivial blurbs to the running text
                        if has_anomalies:
                            validation_text_lines.append(response_text)

                except Exception as batch_err:
                    # Record the error as a span too so it's visible in MLflow
                    try:
                        with mlflow.start_span(
                            name=f"deep_validation_batch_{batch_num}/{total_batches}_error"
                        ) as err_span:
                            err_span.set_attribute("batch_num", batch_num)
                            err_span.set_attribute("error_type", type(batch_err).__name__)
                            err_span.set_attribute("error_message", str(batch_err))
                    except Exception:
                        pass  # Never let tracing break the main pipeline
                    validation_text_lines.append(
                        f"[Batch {batch_num}/{total_batches} error: {type(batch_err).__name__}: {batch_err}]"
                    )


            # Combine all batch text blurbs into a single validation summary
            combined_summary = "\n\n".join(validation_text_lines) if validation_text_lines else "No anomalies detected across all facilities."

            # Return as a structured payload — validation_summary is plain text for the Main LLM
            return json.dumps({
                "query": outer.get("query"),
                "validation_summary": combined_summary,
                "data_coverage_summary": outer.get("data_coverage_summary"),
                "batches_processed": total_batches,
                "total_facilities_analyzed": len(findings),
            }, indent=2)

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
    region: str | None = None,
    city: str | None = None,
    operator_type: str | None = None,
    organization_type: str | None = None,
    facility_type: str | None = None,
    affiliation_type: str | None = None,
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

    SCOPE FILTERS (all optional — apply ONLY what the user explicitly mentioned):
        region:            Restrict to a specific state/region (e.g., 'Greater Accra').
        city:              Restrict to a specific city.
        operator_type:     'private' | 'public'
        organization_type: 'facility' | 'ngo'
        facility_type:     'hospital' | 'clinic' | 'dentist' | 'farmacy' | 'doctor'
        affiliation_type:  'faith-tradition' | 'government' | 'community' |
                           'philanthropy-legacy' | 'academic'

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
    # Scope filters — propagate only when user explicitly mentioned them
    if region:
        payload["region"] = region
    if city:
        payload["city"] = city
    if operator_type:
        payload["operator_type"] = operator_type
    if organization_type:
        payload["organization_type"] = organization_type
    if facility_type:
        payload["facility_type"] = facility_type
    if affiliation_type:
        payload["affiliation_type"] = affiliation_type

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
        raw_result = uc_fn.invoke({"query_json": json.dumps(payload)})

        # UCFunctionToolkit wraps the return in {"format": "SCALAR", "value": "<json-string>"}.
        # Unwrap it so the LLM receives clean, well-formed JSON instead of an escaped string.
        outer = json.loads(raw_result)
        if isinstance(outer, dict) and "value" in outer and "format" in outer:
            inner = outer["value"]
            outer = json.loads(inner) if isinstance(inner, str) else inner

        # The SQL map_from_arrays forces all values to STRING (all values must share one type).
        # Rehydrate numeric metadata fields back to proper Python numbers.
        _float_fields = ("reference_lat", "reference_lon", "radius_km")
        _int_fields   = ("total_facilities_found",)
        for f in _float_fields:
            if f in outer and isinstance(outer[f], str):
                try:
                    outer[f] = float(outer[f])
                except (ValueError, TypeError):
                    pass
        for f in _int_fields:
            if f in outer and isinstance(outer[f], str):
                try:
                    outer[f] = int(outer[f])
                except (ValueError, TypeError):
                    pass

        # The SQL function double-encodes the facilities/cold_spot arrays as a JSON string.
        # Parse it so the result is a proper nested object (not an escaped string).
        for key in ("facilities", "cold_spot_regions"):
            raw_val = outer.get(key)
            if isinstance(raw_val, str):
                try:
                    outer[key] = json.loads(raw_val)
                except (json.JSONDecodeError, TypeError):
                    pass  # leave as-is if not valid JSON

        # Remove empty/null condition_filter to keep the payload lean.
        if outer.get("condition_filter") in ("", "none", None):
            outer.pop("condition_filter", None)

        return json.dumps(outer, indent=2)
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
  "anomal", "gap", "unmet", "outlier", "flag",
  "abnormal", "red flag", "problem type", "workforce",
  "staffing", "correlat", "overlapping", "mismatch",
  "feature mismatch", "procedure count", "equipment count", "signal",
  "validate", "consistency", "verify claim", "capable", "infrastructure",
  "procedure.*equipment", "equipment.*procedure"

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

### Step 2.5 — Vector Search Fact-Type Guide (ALWAYS follow this when calling vector_search_tool):

Each row in `facility_facts` has exactly ONE `fact_type`. Choose `fact_types` based strictly on
what the user is asking about. Do NOT over-fetch — only include types that are directly relevant:

| User is asking about...                              | fact_types to use                      |
|------------------------------------------------------|----------------------------------------|
| What specialties a facility offers                   | ["specialty"]                          |
| What procedures a facility performs                  | ["procedure"]                          |
| What equipment a facility has                        | ["equipment"]                          |
| A facility's opening hours, contact, departments     | ["capability"]                         |
| General overview / type / location of a facility     | ["summary"]                            |
| A facility's background, narrative, or mission       | ["description"]                        |
| Whether procedures match equipment (plausibility)    | ["procedure", "equipment"]             |
| Whether specialties match procedures                 | ["specialty", "procedure"]             |
| Full clinical profile (deep validation / audit)      | ["specialty", "procedure", "equipment"]|
| Contradictions or inconsistencies across all facts   | None (search across all types)         |
| Similarity search ("hospitals like X")               | ["summary", "description", "capability"]|

NEVER pass all 5 fact_types unless contradictions/inconsistencies across all categories are
explicitly asked for. Always pick the minimal relevant set.

### Step 2.5 — Geospatial Protocol (applies when IS_GEOSPATIAL = True):

If the user asks for a physical distance search (e.g., "within 50 km of Accra"), you do NOT need to look up exact coordinates.
Simply pass `reference_location="Accra"` to the `geospatial_query_tool`, and it will dynamically fetch the latitude/longitude for you!

**Choosing `analysis_type`:**
  • "nearby"      — user asks "within X km" or "near" a location. Provide `reference_location` and `radius_km`.
  • "cold_spot"   — user asks about regions lacking a service, geographic gaps.
  • "urban_rural" — user asks about urban vs rural service distribution. You may optionally pass a list of `urban_hubs`
                    (e.g., `urban_hubs=["Accra", "Kumasi"]`) to dynamically define the urban centers for this search.

**Scope Filters for Geospatial Tool:**
If the user specifies any of the following in their query, extract and pass them to `geospatial_query_tool`:
  • operator_type:     'private' or 'public' (e.g., "only public hospitals")
  • organization_type: 'facility' or 'ngo' (e.g., "only NGO facilities")
  • facility_type:     'hospital' | 'clinic' | 'dentist' | 'farmacy' | 'doctor'
  • affiliation_type:  'faith-tradition' | 'government' | 'community' | 'philanthropy-legacy' | 'academic'
  • region:            State/region name (e.g., "Greater Accra" — in addition to or instead of reference_location)
  • city:              City name

**IS_GEOSPATIAL + IS_ANALYTIC Pipeline (CRITICAL — "anomalies within 50km of Accra"):**
When the query is BOTH geospatial AND analytic:
  1. Call `geospatial_query_tool` first to get a list of facilities matching the radius search.
  2. From its JSON output, extract ALL facility_id values from the `facilities` array as a Python list.
  3. Call `medical_agent_tool` with `facility_ids=["id1", "id2", ...]` passing the extracted list.
  This ensures the anomaly/validation analysis runs ONLY on the exact facilities found within the radius.

### Step 2.5 — Medical Agent Tool Branch Selection Guide (CRITICAL):

The `medical_agent_tool` is powered by a backend SQL function that uses EXACT keyword matching (`RLIKE`) on your `query` argument to decide which analysis branch to run. **If you do not include specific keywords, your query may fail or hit the wrong branch!**

When calling `medical_agent_tool`, you MUST include one of the Exact Match Keywords in your `query` parameter depending on your goal:

| Backend Branch | Use When User Asks About... | MUST include at least one exact keyword in `query` |
|---|---|---|
| **Branch 1: Unmet Needs** | Missing specialties or absent procedures in a region | `unmet`, `gap`, `need`, `service gap` |
| **Branch 2: Capacity Outliers** | Unusually high/low bed or doctor numbers | `outlier`, `anomal`, `flag`, `capacity outlier`, `doctor anomaly` |
| **Branch 3: NGO Overlap** | Multiple NGOs operating similarly in the same city | `ngo overlap`, `overlapping ngo`, `same ngo`, `same region` |
| **Branch 4: Problem Type** | Facilities lacking all data for equipment or procedures | `problem type`, `root cause`, `gap type`, `classify gap`, `staff shortage` |
| **Branch 5: Deep Validation** | Verifying claims/mismatches. *(Requires passing a `region` or `facility_name`!)* | `deep valid`, `validate`, `consistency`, `verify claim`, `mismatch`, `feature mismatch`, `procedure count`, `infrastr` |

*Example:* If the user asks "Find hospitals making suspicious surgical claims", DO NOT just use `"suspicious surgical claims"`. You must inject a Branch 5 keyword: `"verify claim for suspicious surgical claims"`.

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
  • For `facility_profile_counts` (Problem Type):
      1. **ALWAYS start** by using `data_coverage_summary` to state the systemic data availability (e.g. "We only have equipment data for X% of facilities").
      2. For each facility, check the `_status` fields (`equipment_status`, `specialty_status`, `procedure_status`):
         - If a status is **`missing_data`**: The database simply lacks records for this category. Do NOT diagnose this as a medical gap. Group these as "Facilities with Unverifiable Missing Data" and explain why they cannot be fully evaluated.
         - If a status is **`true_zero`**: This is a confirmed absence of capability. Classify these true medical gaps as "equipment_gap" (has doctors/specialties but truly 0 equipment), "service_gap" (has equipment but truly 0 services/procedures), or "overclaim_gap" (claims many procedures but has 0 verifiable specialties).
  • For `ngo_overlap_raw`: Evaluate if multiple facilities with the exact same NGO affiliation in the same city represent a duplication of services or complementary care.
  • For `deep_validation` (Verifying claims and capabilities):
      The tool has already analyzed all matching facilities in sequential batches of 15.
      It returns a `validation_summary` — a plain-text block where each anomalous facility
      has a compact blurb: [SEVERITY] [Facility Name] – [core mismatch]. Missing: [...]. [Reasoning] - [brief medical reasoning]
      Consistent facilities are omitted from the summary entirely.
      1. **ALWAYS start** with `data_coverage_summary` — state `total_facilities_analyzed`
         vs. `data_coverage_summary.skipped_insufficient_data`.
      2. Read `validation_summary`. Group facilities by severity: **high** → **medium** → **low**.
      3. Apply the **Handling Large Results** rules from Step 4 to pick table vs. high-level summary.
      4. If `validation_summary` says "No anomalies detected", state this clearly.

### Step 3 — Multi-tool orchestration:

After receiving each tool result, decide:
  • If more data is needed from another tool → call the next tool in sequence
  • If all necessary data is collected → synthesize a comprehensive markdown answer
  • If a tool returns an error or empty results → try the next appropriate tool as fallback
  • Never repeat the same tool twice for the same purpose. DO NOT repeatedly call a tool with slight variations of your search term (e.g. 'procedure count' then 'procedure count anomaly' then 'procedure count outlier'). If the first call returns valid JSON data (even if no anomalies are found), accept it and synthesize the final answer.

### Step 4 — Response format:

• You MUST ALWAYS provide a final, human-readable response in Markdown format after your tool calls are complete.
• NEVER respond with raw JSON, raw tool outputs, or unformatted text as your final answer.
• CRITICAL: NEVER include internal `facility_id` strings, UUIDs, or primary keys in the final Markdown output. Do a final check to ensure NO IDs are printed in text or tables. Keep it clean and user-friendly.
• DO NOT mention internal tools (e.g., "vector-search", "Genie", "medical-agent") or technical query mechanisms in your response. Present your answers naturally to the user.
• If you called multiple tools, synthesize their results together into a single cohesive summary.

### Handling Large Results:
If a tool (like geospatial search, medical anomaly flagging, or deep validation) returns a large list of facilities, STRICTLY follow these rules based on the number of **relevant/anomalous** facilities you find after your analysis:

1. **If there are ≤ 25 relevant/anomalous facilities**:
   Display ALL of them in a comprehensive markdown table.

2. **If there are between 26 and 60 relevant/anomalous facilities**:
   List the **top 60 most important/anomalous** facilities in a markdown table (prioritize by severity: high → medium → low, or by distance if geospatial). Then append the high-level summary below the table.

3. **If there are > 60 relevant/anomalous facilities**:
   List the **top 60 most important/anomalous** facilities in a markdown table (same priority rule). Then append the high-level summary below the table. 
   NEVER list all facilities. NEVER omit the table entirely.

**CRITICAL: You MUST include a markdown table with AT LEAST 20 facilities ONLY IF there are actually ≥ 20 truly relevant/anomalous facilities. If the tool returns 70 rows but only 12 have genuine anomalies/relevance, just list those 12 in the table. DO NOT pad the table with normal/consistent facilities just to hit 20. But if there ARE ≥ 20 valid hits, you MUST table them.**

### High-Level Summary Template (always append after the table when > 25 rows):
(Use paragraphs and bullet points. NEVER put this summary inside a markdown table):
  
  Answer: There are [Total Number] [Facility Type/Hospitals] [condition/radius/finding].
  
  Key findings:
  - **Most Extreme / Highest Severity**: [Name] – [finding].
  - **Other notable facilities**: [Name 1], [Name 2], [Name 3] and [Name 4] ([brief note on why]).
  
  Medical context: [1-2 sentences on the medical implications of this density/anomaly]


• Cite specific facility names and regions.
• If no results are found, say so clearly and suggest trying a different approach.


### Step 5 — Missing Information:
If the user asks a question that requires a region or city (such as finding nearby facilities, generating a specific regional anomaly report, or filtering by distance) but they DO NOT mention any region or city in their prompt, you MUST explicitly ask the user to provide the region or city before proceeding. Do NOT assume a default region. Use your interactive capability to clarify their request.
"""


# ─── LangGraph ────────────────────────────────────────────────────────────────

class AgentState(TypedDict):
    messages: Annotated[Sequence, add_messages]


llm = ChatDatabricks(endpoint=LLM_ENDPOINT, temperature=0.1, max_tokens=16384)
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
        facility_facts has NO lat/lon — coordinates are only in facility_records.
        We extract facility_id here; the frontend can enrich coords from /map/facility/{id}.
        """
        sources: list[dict[str, Any]] = []
        try:
            import re
            # Primary: try JSON parse if output was serialised
            try:
                docs = json.loads(raw_output)
                if isinstance(docs, list):
                    for doc in docs:
                        meta = doc.get("metadata", {}) if isinstance(doc, dict) else {}
                        page_content = doc.get("page_content", "") if isinstance(doc, dict) else ""
                        snippet = page_content[:200] + "..." if len(page_content) > 200 else page_content
                        sources.append({
                            "source_type": "facility_facts",
                            "fact_id": meta.get("fact_id"),
                            "facility_id": meta.get("facility_id"),
                            "fact_type": meta.get("fact_type"),
                            "excerpt": snippet,
                            # No lat/lon — facility_facts table has none
                        })
                    return {
                        "step_index": None,
                        "tool_name": call_name,
                        "call_id": call_id,
                        "query_used": call_args.get("query", ""),
                        "tables_accessed": ["facility_facts"],
                        "sources": sources,
                    }
            except (json.JSONDecodeError, TypeError):
                pass

            # Fallback: regex on Python repr of LangChain Document objects
            doc_pattern = re.compile(
                r"metadata=\{([^}]+)\}.*?page_content='(.*?)(?='\s*\))",
                re.DOTALL,
            )
            for m in doc_pattern.finditer(raw_output):
                meta_str = m.group(1)
                page_content = m.group(2).strip()
                meta: dict[str, str] = {}
                for kv in re.finditer(r"'(\w+)':\s*'([^']*)'", meta_str):
                    meta[kv.group(1)] = kv.group(2)
                snippet = page_content[:200] + "..." if len(page_content) > 200 else page_content
                # The excerpt always starts with "FacilityName in City, ..." — extract the name.
                facility_name_extracted = None
                if " in " in snippet:
                    facility_name_extracted = snippet.split(" in ")[0].strip()
                sources.append({
                    "source_type": "facility_facts",
                    "fact_id": meta.get("fact_id"),
                    "facility_id": meta.get("facility_id"),
                    "facility_name": facility_name_extracted,
                    "fact_type": meta.get("fact_type"),
                    "excerpt": snippet,
                })
        except Exception as _genie_parse_err:
            import warnings as _w
            _w.warn(
                f"[CitationParser] genie_chat parse failed (call_id={call_id}): "
                f"{type(_genie_parse_err).__name__}: {_genie_parse_err}"
            )
        return {
            "step_index": None,
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
        Now also extracts latitude/longitude from enriched SQL branches (1, 4, 5, 7, 8).
        """
        sources: list[dict[str, Any]] = []
        tables_accessed: set[str] = {"facility_records"}
        try:
            data = json.loads(raw_output)
            # UCFunctionToolkit wraps results in {"format": "SCALAR", "value": "<json-string>"}.
            # Unwrap to get the actual SQL return value before reading any keys.
            if isinstance(data, dict) and "value" in data and "format" in data:
                inner = data["value"]
                data = json.loads(inner) if isinstance(inner, str) else inner

            # Standard findings (non-deep-validation branches)
            findings = data.get("findings") or []
            if isinstance(findings, str):
                findings = json.loads(findings)

            # validation_summary is now plain text — no structured citations possible.
            # We skip it here; the Main LLM reads it directly.
            val_results = []

            all_items = findings + val_results

            for f in all_items:
                if not isinstance(f, dict):
                    continue
                finding_type = f.get("type") or ("deep_validation" if "status" in f else "")
                # Determine which tables this finding draws from
                if finding_type in ("contradictory_signals", "feature_mismatch_raw",
                                    "ngo_raw_data", "ngo_overlap_raw"):
                    tables_accessed.add("facility_facts")
                if finding_type in ("regional_coverage",):
                    tables_accessed.add("regional_insights")

                # ── All other finding types (generic single-facility or regional) ────
                source: dict[str, Any] = {
                    "source_type": "facility_records",
                    "finding_type": finding_type,
                    "facility_id": f.get("facility_id") or f.get("region"),
                    "facility_name": f.get("facility_name") or f.get("region"),
                    "latitude": f.get("latitude"),
                    "longitude": f.get("longitude"),
                    "severity": f.get("severity"),
                    "note": f.get("note") or f.get("reason") or f.get("recommendation"),
                }
                if finding_type == "regional_coverage":
                    source["source_type"] = "regional_insights"
                    source["latitude"] = None
                    source["longitude"] = None
                    source["region"] = f.get("region")
                    source["total_facilities"] = f.get("total_facilities")
                sources.append(source)
        except Exception as _med_parse_err:
            import warnings as _w
            _w.warn(
                f"[CitationParser] medical_agent parse failed (call_id={call_id}): "
                f"{type(_med_parse_err).__name__}: {_med_parse_err}"
            )
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
        Genie returns either free-text or a structured table (rows + columns).
        We attempt to parse the structured table format first to extract facility rows.
        Genie queries facility_records and/or regional_insights — never facility_facts.
        NOTE: Genie output has no lat/lon — Genie does SELECT on aggregated data.
        The frontend requests coords from /map/facility/{id} using extracted facility_ids.
        """
        sources: list[dict[str, Any]] = []
        tables_accessed: list[str] = []
        raw_lower = raw_output.lower() if isinstance(raw_output, str) else ""

        # Detect accessed tables from keywords in the response text
        if any(w in raw_lower for w in ["facility", "hospital", "clinic", "dentist", "doctor", "farmacy"]):
            tables_accessed.append("facility_records")
        if any(w in raw_lower for w in ["region", "state", "district", "insight", "coverage"]):
            tables_accessed.append("regional_insights")
        if not tables_accessed:
            tables_accessed = ["facility_records"]

        # Attempt to parse Genie structured output: may be a dict with 'columns' and 'data'
        try:
            # Genie sometimes returns: {'columns': [...], 'data': [[val1, val2, ...], ...]}
            parsed = json.loads(raw_output) if isinstance(raw_output, str) else raw_output
            if isinstance(parsed, dict):
                columns = parsed.get("columns") or parsed.get("schema", {}).get("fields", [])
                rows = parsed.get("data") or parsed.get("result", [])
                if columns and rows:
                    col_names = [c if isinstance(c, str) else c.get("name", f"col_{i}") for i, c in enumerate(columns)]
                    for row in rows[:20]:  # cap at 20 rows to avoid huge citations
                        row_dict = dict(zip(col_names, row)) if isinstance(row, list) else row
                        sources.append({
                            "source_type": "genie_row",
                            "facility_id": row_dict.get("facility_id"),
                            "facility_name": row_dict.get("facility_name"),
                            "city": row_dict.get("city"),
                            "state": row_dict.get("state"),
                            # No lat/lon — Genie SELECT does not include geospatial columns
                        })
        except (json.JSONDecodeError, TypeError, AttributeError):
            pass

        # Fallback: capture a text snippet for provenance
        if not sources:
            snippet = raw_output[:400] + "..." if isinstance(raw_output, str) and len(raw_output) > 400 else str(raw_output)
            sources = [{
                "source_type": "genie_response",
                "tables_queried": tables_accessed,
                "excerpt": snippet,
            }]

        return {
            "step_index": None,
            "tool_name": call_name,
            "call_id": call_id,
            "query_used": call_args.get("query", ""),
            "tables_accessed": tables_accessed,
            "sources": sources,
        }

    @staticmethod
    def _parse_geospatial_citations(call_id: str, call_name: str,
                                    call_args: dict, raw_output: str) -> dict[str, Any]:
        """
        Parse geospatial_query_tool output.
        The SQL function wraps results in a top-level JSON object:
          { 'analysis_type': '...', 'facilities': '[{...}, ...]'  }  (nearby / urban_rural)
          { 'analysis_type': 'cold_spot', 'cold_spot_regions': '[{...}]' }  (cold_spot)
        The inner 'facilities' value is itself a JSON-encoded string in some SDK versions.
        """
        sources: list[dict[str, Any]] = []
        analysis_type = "nearby"
        try:
            outer = json.loads(raw_output)
            analysis_type = outer.get("analysis_type", "nearby")

            if analysis_type == "cold_spot":
                regions_raw = outer.get("cold_spot_regions", "[]")
                regions = json.loads(regions_raw) if isinstance(regions_raw, str) else regions_raw
                for r in (regions or []):
                    if not isinstance(r, dict):
                        continue
                    source_dict = {
                        "source_type": "facility_records",
                        "region": r.get("state"),
                        "total_facilities": r.get("total_facilities"),
                        "matching_facilities": r.get("matching_facilities"),
                    }
                    sources.append({k: v for k, v in source_dict.items() if v is not None})
            else:
                # nearby or urban_rural
                facilities_raw = outer.get("facilities", "[]")
                facilities = json.loads(facilities_raw) if isinstance(facilities_raw, str) else facilities_raw
                for r in (facilities or []):
                    if not isinstance(r, dict):
                        continue
                    source_dict = {
                        "source_type": "facility_records",
                        "facility_id": r.get("facility_id"),
                        "facility_name": r.get("facility_name"),
                        "city": r.get("city"),
                        "state": r.get("state"),
                        "distance_km": r.get("distance_km")
                    }
                    sources.append({k: v for k, v in source_dict.items() if v is not None})
        except Exception:
            pass
        return {
            "step_index": None,
            "tool_name": call_name,
            "call_id": call_id,
            "query_used": str(call_args),
            "tables_accessed": ["facility_records"],
            "analysis_type": analysis_type,
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
                # Try direct facility_name key first (set by all parsers)
                name = src.get("facility_name")
                # Fallback: extract from excerpt (format: "FacilityName in City, ...")
                if not name:
                    excerpt = src.get("excerpt", "")
                    if excerpt and " in " in excerpt:
                        name = excerpt.split(" in ")[0].strip()
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
        from mlflow.entities import SpanType

        # Extract user question for the MLflow UI
        user_input = ""
        if request.input:
            last_in = request.input[-1]
            user_input = getattr(last_in, "content", "")

        # Wrap execution in a root span formatted exactly how MLflow's Chat UI expects.
        with mlflow.start_span(name="MedAtlas Agent", span_type=SpanType.CHAT_MODEL) as root_span:
            root_span.set_inputs({"messages": [{"role": "user", "content": user_input}]})

            tracker = _ToolCallTracker()
            events = self._run_graph(request, tracker)
            outputs = [e.item for e in events if e.type == "response.output_item.done"]

            # Extract final markdown response for the MLflow UI
            last_message = ""
            for out in reversed(outputs):
                if out.type == "message" and out.role == "assistant":
                    if getattr(out, "content", None) and isinstance(out.content, list):
                        last_message = getattr(out.content[0], "text", "")
                    break

            root_span.set_outputs({
                "choices": [{"message": {"role": "assistant", "content": last_message}}]
            })

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
