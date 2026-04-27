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
  - Single LangGraph graph: [agent] → [tools] → [tool_postprocessor] → [agent]
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

# experiment_id = os.getenv("MLFLOW_EXPERIMENT_ID")
# tracking_uri = os.getenv("MLFLOW_TRACKING_URI")
# registry_uri = os.getenv("MLFLOW_REGISTRY_URI")
# Optional overrides:
# - In Databricks Apps, experiment resource injection is enough for tracking.
# - For local remote tracking, you can still set MLFLOW_TRACKING_URI / auth env vars.
# if tracking_uri:
#     mlflow.set_tracking_uri(tracking_uri)

# # Registry URI selection:
# # - Honor explicit MLFLOW_REGISTRY_URI.
# # - Default to Unity Catalog registry when using Databricks tracking.
# if registry_uri:
#     mlflow.set_registry_uri(registry_uri)

# try:
#     if experiment_id:
#         mlflow.set_experiment(experiment_id=experiment_id)
#     else:
#         warnings.warn(
#             "No MLflow experiment configured. Set MLFLOW_EXPERIMENT_ID (recommended in Databricks Apps)."
#         )
# except mlflow.exceptions.MlflowException as exc:
#     warnings.warn(
#         f"MLflow experiment setup failed ({exc}). Continuing without forcing experiment selection."
#     )

mlflow.set_tracking_uri("sqlite:///mlflow.db")
try:
    mlflow.set_experiment("Default")
except mlflow.exceptions.MlflowException as exc:
    warnings.warn(f"Failed to set local MLflow experiment to Default: {exc}")
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
    Content
)

# Databricks integrations
from databricks_langchain import ChatDatabricks

# ─── Configuration ─────────────────────────────────────────────────────────────

LLM_ENDPOINT  = os.environ["LLM_ENDPOINT"]
VS_INDEX     = os.environ.get("VECTOR_SEARCH_INDEX")
GENIE_ID     = os.environ["GENIE_SPACE_ID"]
CATALOG      = os.environ.get("CATALOG")
SCHEMA       = os.environ.get("SCHEMA", "default")

# Prefer resource-injected UC function names (Databricks Apps), with fallback for local/dev.
ANALYZE_UC_FUNCTION_NAME = os.environ.get("ANALYZE_UC_FUNCTION_NAME")
if not ANALYZE_UC_FUNCTION_NAME and CATALOG and SCHEMA:
    ANALYZE_UC_FUNCTION_NAME = f"{CATALOG}.{SCHEMA}.analyze_medical_query"

GEOSPATIAL_UC_FUNCTION_NAME = os.environ.get("GEOSPATIAL_UC_FUNCTION_NAME")
if not GEOSPATIAL_UC_FUNCTION_NAME and CATALOG and SCHEMA:
    GEOSPATIAL_UC_FUNCTION_NAME = f"{CATALOG}.{SCHEMA}.find_facilities_nearby"


# ─── Domain constants ──────────────────────────────────────────────────────────

GHANA_REGIONS: list[str] = [
    "Ahafo", "Greater Accra", "Western", "Eastern", "Ashanti",
    "Volta", "Central", "Bono East", "Northern", "Western North",
    "Oti", "Bono", "North East", "Savannah", "Upper West", "Upper East",
]

# Critical life-saving procedures used as the VS query in cold-spot analysis.
# Covers high-burden interventions most frequently absent in low-resource settings.
CRITICAL_PROCEDURES: list[str] = [
    "caesarean section",
    "blood transfusion",
    "open heart surgeries",
    "kidney transplant surgeries",
    "renal dialysis treatment",
    "cataract surgery",
    "cornea transplant",
    "vitrectomy",
    "obstetric fistula repair",
    "laparotomy for ectopic gestations",
    "endoscopic retrograde cholangiopancreatography (ERCP)",
    "glaucoma surgeries",
    "general surgery",
    "safe abortion and post-abortion care",
    "anaesthesia services",
]

# Single VS query string that finds facilities offering ANY critical procedure.
_COLD_SPOT_PROCEDURE_QUERY: str = " ".join(CRITICAL_PROCEDURES)

# Human-message keywords that signal a cold-spot (ABSENCE) query vs. a standard
# GEO+SEMANTIC (PRESENCE) query. Checked by the postprocessor.
_COLD_SPOT_KEYWORDS: frozenset[str] = frozenset([
    "cold spot", "cold-spot", "coldspot",
    "absent", "absence", "lacking",
    "no access", "coverage gap",
    "procedure missing", "service absent",
    "travel time", "hours away",
    "where is", "where are there no",
])


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
        "num_results": 100,
        # fact_id is the VS index primary key — MUST be included for retrieval to work.
        # It is intentionally excluded from the JSON output returned to the LLM (see below).
        "columns": ["fact_id", "facility_id", "fact_text", "fact_type"],
    }

    if fact_types and len(fact_types) == 1:
        kwargs["filters"] = {"fact_type": fact_types[0]}
    elif fact_types and len(fact_types) > 1:
        # Databricks VS multi-value filter: pass list directly as the value (NOT MongoDB $in syntax)
        kwargs["filters"] = {"fact_type": fact_types}

    post_filter_types = set(fact_types) if fact_types and len(fact_types) > 1 else None

    try:
        vs = VectorSearchRetrieverTool(**kwargs)
        results = vs.invoke({"query": query})
        
        print("\n" + "="*80)
        print("DEBUG: RAW VS INVOKE RESULTS (before JSON structuring):")
        print(results)
        print("="*80 + "\n")
        
        if post_filter_types and isinstance(results, list):
            results = [
                doc for doc in results
                if doc.metadata.get("fact_type") in post_filter_types
            ]
        # Convert Document list to structured JSON — drop fact_id (not needed by LLM)
        if isinstance(results, list):
            structured = [
                {
                    "facility_id": doc.metadata.get("facility_id", ""),
                    "fact_type":   doc.metadata.get("fact_type", ""),
                    "fact_text":   doc.page_content,
                }
                for doc in results
            ]
            return json.dumps({"results": structured, "total_results": len(structured)}, indent=2)
        return json.dumps({"results": [], "total_results": 0})
    except Exception as exc:
        return f"[Vector Search Error] {exc}"


# ─── Tool 3 — Medical Agent ───────────────────────────────────────────────────

# Batch size for deep validation LLM calls
_DEEP_VALIDATION_BATCH_SIZE = 20

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

    Returns data for 4 analysis types:
      1. regional_coverage        — per-region service coverage arrays for LLM gap analysis
      2. anomaly_flagging         — outlier capacity/doctor counts (3 std devs, global baseline)
      3. ngo_overlap_raw          — NGOs grouped by affiliation+region for LLM overlap analysis
      4. deep_validation          — region-scoped specialty↔procedure↔equipment consistency check
                                    (batched internally)
                                    REQUIRES: region OR facility_name OR facility_id

    NOTE — For classification/breakdown queries use genie_chat_tool instead.
    NOTE — For contradiction detection, use vector_search_tool instead.

    IMPORTANT — For branches that return raw data (types 1, 2, 3), YOU must synthesize
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
    "overlapping", "corrobor", "mismatch", "feature mismatch",
    "procedure count", "equipment count",
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
        if not ANALYZE_UC_FUNCTION_NAME:
            return (
                "[Medical Agent Error] Missing UC function name. Set ANALYZE_UC_FUNCTION_NAME "
                "or set CATALOG/SCHEMA for fallback resolution."
            )
        uc = UCFunctionToolkit(
            function_names=[ANALYZE_UC_FUNCTION_NAME]
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

        # Also parse data_coverage_summary (Branches 2 and 4 return it as a JSON string)
        cov_raw = outer.get("data_coverage_summary")
        if isinstance(cov_raw, str):
            try:
                outer["data_coverage_summary"] = json.loads(cov_raw)
            except (json.JSONDecodeError, TypeError):
                pass  # keep as-is if not valid JSON


        # ── Batched LLM Evaluation ────────────────────────────────────────
        # Only deep_validation requires Python-level batching.
        # Feature mismatch is now fully handled inside Branch 4's deep_validation path.
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
    reference_location: str,
    radius_km: float = 50.0,
    facility_type: str | None = None,
    operator_type: str | None = None,
    organization_type: str | None = None,
    affiliation_type: str | None = None,
    scan_all_ghana_regions: bool = False,
) -> str:
    """
    Geospatial facility search using ST_DistanceSpheroid on the WGS84 spheroid.
    Geocodes `reference_location` via LocationIQ to obtain precise lat/lon,
    then queries the Unity Catalog SQL function for all facilities within radius_km.

    Returns: A list of up to 100 facilities within radius_km, sorted by ascending distance.

    Args:
        reference_location:       REQUIRED. Name of the city or region to center the search on
                                  (e.g., "Accra", "Kumasi", "Volta region"). The tool
                                  geocodes this automatically via LocationIQ — do NOT pass
                                  raw lat/lon coordinates.
        radius_km:                Search radius in kilometres (default 50).
        facility_type:            Optional. 'hospital' | 'clinic' | 'dentist' | 'farmacy' | 'doctor'.
                                  Only pass if the user explicitly mentioned this.
        operator_type:            Optional. 'private' | 'public'.
                                  Only pass if the user explicitly mentioned this.
        organization_type:        Optional. 'facility' | 'ngo'.
                                  Only pass if the user explicitly mentioned this.
        affiliation_type:         Optional. 'faith-tradition' | 'government' | 'community' |
                                  'philanthropy-legacy' | 'academic'.
                                  Only pass if the user explicitly mentioned this.
        scan_all_ghana_regions:   Set to True for global cold-spot analysis when the user
                                  asks about cold spots across ALL of Ghana without specifying
                                  a particular location. Geocodes each of the 16 Ghana
                                  regional capitals via LocationIQ and returns the
                                  deduplicated union of all facilities within radius_km of each.

    CRITICAL: NEVER pass ref_lat or ref_lon — those parameters no longer exist.
    Always pass `reference_location` as a plain string from the user’s prompt.

    Trigger keywords: "within", "km", "distance", "near", "nearby", "closest",
    "cold spot", "geographic", "radius", "proximity", "urban", "rural".
    """
    from unitycatalog.ai.langchain.toolkit import UCFunctionToolkit
    import requests
    import time
    import os

    # ── GLOBAL SCAN MODE ─────────────────────────────────────────────────────────
    # When scan_all_ghana_regions=True, geocode all 16 Ghana regions and return
    # the deduplicated union of all facilities found within radius_km of each.
    if scan_all_ghana_regions:
        api_key = os.getenv("LOCATION_IQ_ACCESS_TOKEN")
        if not api_key:
            return "[Geospatial Query Error] LOCATION_IQ_ACCESS_TOKEN not set."

        all_facilities_by_id: dict[str, dict] = {}
        regions_successful: list[str] = []
        regions_failed: list[str] = []

        for _region in GHANA_REGIONS:
            try:
                _resp = requests.get(
                    "https://us1.locationiq.com/v1/search",
                    params={"key": api_key, "q": f"{_region} region, Ghana", "format": "json"},
                    timeout=5,
                )
                time.sleep(0.5)  # rate-limit guard: stay within 2 req/sec
                if _resp.status_code != 200 or not _resp.json():
                    regions_failed.append(_region)
                    continue
                _lat = float(_resp.json()[0]["lat"])
                _lon = float(_resp.json()[0]["lon"])
            except Exception:
                regions_failed.append(_region)
                continue

            _payload: dict = {"ref_lat": _lat, "ref_lon": _lon, "radius_km": radius_km}
            if operator_type:     _payload["operator_type"]     = operator_type
            if organization_type: _payload["organization_type"] = organization_type
            if facility_type:     _payload["facility_type"]     = facility_type
            if affiliation_type:  _payload["affiliation_type"]  = affiliation_type

            try:
                if not GEOSPATIAL_UC_FUNCTION_NAME:
                    continue
                _uc = UCFunctionToolkit(function_names=[GEOSPATIAL_UC_FUNCTION_NAME])
                _raw = _uc.tools[0].invoke({"query_json": json.dumps(_payload)})
                _outer = json.loads(_raw)
                if isinstance(_outer, dict) and "value" in _outer and "format" in _outer:
                    _inner = _outer["value"]
                    _outer = json.loads(_inner) if isinstance(_inner, str) else _inner
                _facs_raw = _outer.get("facilities", [])
                if isinstance(_facs_raw, str):
                    _facs_raw = json.loads(_facs_raw)
                for _fac in (_facs_raw if isinstance(_facs_raw, list) else []):
                    _fid = _fac.get("facility_id")
                    if _fid and _fid not in all_facilities_by_id:
                        all_facilities_by_id[_fid] = _fac
                regions_successful.append(_region)
            except Exception:
                regions_failed.append(_region)
                continue

        facilities_list = list(all_facilities_by_id.values())
        return json.dumps({
            "scan_type":                "all_ghana_regions",
            "radius_km":               radius_km,
            "regions_scanned":         regions_successful,
            "regions_failed":          regions_failed,
            "total_facilities_returned": len(facilities_list),
            "facilities":              facilities_list,
        }, indent=2)

    # ── SINGLE LOCATION MODE (geocoding always mandatory) ────────────────────────
    # Always geocode the reference_location string via LocationIQ.
    # The LLM must NEVER pass ref_lat/ref_lon directly.
    api_key = os.getenv("LOCATION_IQ_ACCESS_TOKEN")
    if not api_key:
        return "[Geospatial Query Error] LOCATION_IQ_ACCESS_TOKEN not set in environment."
    try:
        resp = requests.get(
            "https://us1.locationiq.com/v1/search",
            params={"key": api_key, "q": f"{reference_location}, Ghana", "format": "json"},
            timeout=5,
        )
        time.sleep(0.5)  # rate-limit guard: stay within 2 req/sec
        if resp.status_code == 200 and len(resp.json()) > 0:
            ref_lat = float(resp.json()[0]["lat"])
            ref_lon = float(resp.json()[0]["lon"])
        else:
            return f"[Geospatial Query Error] Could not dynamically geocode '{reference_location}'."
    except Exception as e:
        return f"[Geospatial Query Error] Geocoding failed: {e}"

    payload: dict = {
        "ref_lat":   ref_lat,
        "ref_lon":   ref_lon,
        "radius_km": radius_km,
    }
    # Attribute-level scope filters only (no city/region — geocoordinates handle geography)
    if facility_type:     payload["facility_type"]     = facility_type
    if operator_type:     payload["operator_type"]     = operator_type
    if organization_type: payload["organization_type"] = organization_type
    if affiliation_type:  payload["affiliation_type"]  = affiliation_type

    try:
        if not GEOSPATIAL_UC_FUNCTION_NAME:
            return (
                "[Geospatial Query Error] Missing UC function name. Set GEOSPATIAL_UC_FUNCTION_NAME "
                "or set CATALOG/SCHEMA for fallback resolution."
            )
        uc = UCFunctionToolkit(
            function_names=[GEOSPATIAL_UC_FUNCTION_NAME]
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
        _int_fields   = ("total_facilities_returned",)
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

        # The SQL function double-encodes the facilities array as a JSON string.
        # Parse it so the result is a proper nested object (not an escaped string).
        for key in ("facilities",):
            raw_val = outer.get(key)
            if isinstance(raw_val, str):
                try:
                    outer[key] = json.loads(raw_val)
                except (json.JSONDecodeError, TypeError):
                    pass  # leave as-is if not valid JSON

        return json.dumps(outer, indent=2)
    except Exception as exc:
        return f"[Geospatial Query Error] {exc}"


# ─── Tool list ────────────────────────────────────────────────────────────────

ALL_TOOLS = [genie_chat_tool, vector_search_tool, medical_agent_tool, geospatial_query_tool]


# ─── System prompt ─────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are Med-Atlas-AI, a healthcare infrastructure analyst for Ghana.

## Tool Routing — Step-by-Step Decision

Before answering, determine the query type by checking which keywords are present.
Classify the query, then pick the SINGLE best tool using the priority table in Step 2.
Only combine tools when the three explicitly approved multi-tool pipelines apply (GEO+SEMANTIC, GEO+ANALYTIC, or SEMANTIC+ANALYTIC).

### Step 1 — Classify the query (check all three):

IS_GEOSPATIAL = True if ANY of these keywords appear:
  "within", "km", "distance", "near", "nearby", "closest",
  "cold spot", "geographic", "radius", "proximity", "how far", "geospatial", "location-based"

IS_QUANTITATIVE = True if ANY of these keywords appear:
  "how many", "count", "total", "average", "sum", "most", "least",
  "top N", "region", "district", "ownership", "beds", "capacity"
  "ratio", "percentage", "ranking", "compar", "distribution",
  "number of", "how many facilities",
  "specialist", "specialist distribution",
  "web presence", "website", "online presence",
  "doctors", "doctor count", "total doctors", "number of doctors",
  "affiliation", "facility type", "operator type", "organization type"

  NOTE — IS_QUANTITATIVE does NOT apply when the query is about equipment, procedures,
  or capabilities. Those always route to IS_SEMANTIC or IS_ANALYTIC regardless of counting words.

IS_SEMANTIC = True if ANY of these keywords appear:
  "similar", "like", "service", "equipment", "provides", "specialty",
  "has", "can provide", "offers", "what does", "which facilities provide",
  "capability", "capabilities", "similar to", "what services", "procedures", "subspecialty", "camp", "medical camp"

IS_ANALYTIC = True if ANY of these keywords appear:
  "anomal", "gap", "unmet", "outlier", "flag", "oversupply", "scarcity",
  "abnormal", "red flag", "correlat", "overlapping", "mismatch",
  "feature mismatch", "procedure count", "equipment count", "signal",
  "validate", "consistency", "verify claim", "capable", "infrastructure",
  "over-claim", "implausib", "corrobor", "contradict", "inconsisten", "conflicting", "conflict",
  "classify", "categorize", "breakdown", "ngo", "classification",
  "procedure.*equipment", "equipment.*procedure"

IS_COLDSPOT = True if ALL of these conditions hold:
  (a) IS_GEOSPATIAL is True (a radius/distance/location is mentioned)
  AND
  (b) ANY of these absence-keywords appear:
      "cold spot", "cold-spot", "coldspot", "absent", "absence",
      "lacking", "no access", "coverage gap", "procedure missing",
      "service absent", "travel time", "hours away"
  AND
  (c) The user is asking which areas LACK a procedure/service (not which areas have it).

  KEY DISAMBIGUATION (IS_COLDSPOT vs IS_GEOSPATIAL + IS_SEMANTIC):
    "hospitals within 200km that HAVE X-ray" → IS_GEOSPATIAL + IS_SEMANTIC (Priority 2★)
    "cold spots / where X-ray is ABSENT within 200km" → IS_COLDSPOT (Priority 1★)

### Step 2 — Route by priority (pick the SINGLE highest-priority tool; only the four starred pipelines use multiple tools):

| Priority | Classification                  | Tool to Use                                                               |
|----------|---------------------------------|---------------------------------------------------------------------------|
| 1 ★      | IS_COLDSPOT                     | geospatial_query_tool → vector_search_tool (Cold-Spot pipeline below)    |
| 2 ★      | IS_GEOSPATIAL + IS_SEMANTIC     | geospatial_query_tool → vector_search_tool (intersection pipeline below)  |
| 3 ★      | IS_GEOSPATIAL + IS_ANALYTIC     | geospatial_query_tool → medical_agent_tool (facility_ids pipeline below)  |
| 4        | IS_GEOSPATIAL only              | geospatial_query_tool                                                     |
| 5        | IS_ANALYTIC (any combo)         | medical_agent_tool                                                        |
| 6        | IS_SEMANTIC only                | vector_search_tool                                                        |
| 7        | IS_QUANTITATIVE only            | genie_chat_tool                                                           |

**CRITICAL rules:**
- IS_COLDSPOT is highest priority when both IS_GEOSPATIAL is True AND absence keywords are present.
- IS_GEOSPATIAL + IS_SEMANTIC (Priority 2★): geospatial + a service the user wants facilities to HAVE.
- IS_COLDSPOT (Priority 1★): geospatial + asking where a service is ABSENT.
- Plain IS_ANALYTIC without semantic service terms → Priority 5 (medical_agent only).
- `genie_chat_tool` is ONLY called when IS_QUANTITATIVE is True and IS_ANALYTIC and IS_SEMANTIC are both False.
- Do NOT chain genie + vector_search, genie + medical_agent, or all three tools together — these combinations are never correct.

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

**CRITICAL RULES for `geospatial_query_tool`:**
1. Always pass `reference_location` as a plain location name string (e.g., `"Accra"`, `"Kumasi"`, `"Volta region"`).
   The tool geocodes this via LocationIQ internally — you must NEVER supply raw coordinates. `ref_lat`/`ref_lon` no longer exist.
   ✅ `geospatial_query_tool(reference_location="Accra", radius_km=200)`
   ❌ `geospatial_query_tool(ref_lat=5.6, ref_lon=-0.1, radius_km=200)`

2. Do NOT pass `city` or `region` as separate parameters. The geocoded lat/lon already handles geographic
   resolution — adding a city/region string filter ON TOP of a radius double-restricts results incorrectly.
   ✅ `geospatial_query_tool(reference_location="Kumasi", radius_km=50, facility_type="hospital")`
   ❌ `geospatial_query_tool(reference_location="Kumasi", radius_km=50, city="Kumasi")`

3. Only pass `facility_type`, `operator_type`, `organization_type`, or `affiliation_type` when the user
   explicitly mentioned them. These attribute filters are applied ON TOP of the distance filter.

The tool returns up to 100 facilities sorted by ascending distance.

**IS_COLDSPOT Pipeline (Priority 1★ — e.g., "cold spots where critical procedure is absent within X km"):**
When IS_COLDSPOT = True:

  **Case A — Location IS specified** (e.g., "cold spots within 50km of Accra"):
  1. Call `geospatial_query_tool` with `reference_location="Accra"` and `radius_km=50`.
  2. Call `vector_search_tool` with the critical-procedures query (see below) and `fact_types=["procedure", "equipment"]`.
  3. Postprocessor produces `GEO_COLDSPOT_ANALYSIS` — use BOTH fields for your answer:
       - `matched_facilities[]`   → the facilities that ARE covered (have at least one critical procedure)
       - `cold_spot_by_region{}` → region-level counts of covered vs uncovered facilities

  **Case B — No location specified** (e.g., "where are the largest cold spots within 50km"):
  DO NOT ask the user for a location. Instead:
  1. Call `geospatial_query_tool` with `scan_all_ghana_regions=True` and `radius_km=<user_value_or_50>`.
     This internally geocodes all 16 Ghana regions and returns the deduplicated union of all
     facilities found within radius_km of each regional capital.
  2. Call `vector_search_tool` with the critical-procedures query and `fact_types=["procedure", "equipment"]`.
  3. Postprocessor produces `GEO_COLDSPOT_ANALYSIS` — use BOTH fields for your answer:
       - `matched_facilities[]`   → the facilities that ARE covered (have at least one critical procedure)
       - `cold_spot_by_region{}` → region-level counts of covered vs uncovered facilities

  **Critical-procedures VS query for all IS_COLDSPOT calls:**
       query      = "caesarean section blood transfusion open heart surgeries kidney transplant surgeries
                     renal dialysis treatment cataract surgery cornea transplant vitrectomy
                     obstetric fistula repair laparotomy for ectopic gestations
                     endoscopic retrograde cholangiopancreatography glaucoma surgeries
                     general surgery safe abortion and post-abortion care anaesthesia services"
       fact_types = ["procedure", "equipment"]

  **Default radius_km**: 50km if the user did not specify a distance.
  The `vector_search_tool` result will contain only a short system note — ignore it entirely.
  CRITICAL: NEVER call `medical_agent_tool` for cold-spot queries.

  **HOW TO FORMAT YOUR COLD-SPOT RESPONSE (mandatory structure):**

  Your response MUST contain ALL of the following sections:

  **Section 1 — Overall Summary**
  State total facilities in radius, how many are covered vs cold spots, and the overall coverage %.

  **Section 2 — Regional Gap Table**
  A markdown table using `cold_spot_by_region` showing: Region | Facilities in radius | Covered | Cold spots | Coverage %.
  Sort by cold_spot count descending (largest gaps first).

  **Section 3 — Covered Facilities & Top Cold-Spot Facilities per Region**

  **3a — Covered Facilities ("Islands of Coverage")**
  List EVERY facility from `matched_facilities[]` that IS providing a critical procedure.
  For each covered facility, state:
    - Facility name, city, region, distance_km
    - The specific critical procedure(s) it provides — extract this from `matched_facts[].fact_text` in the facility object.
  Group by region. Example:
    **Greater Accra (8 covered)**
    - Korle Bu Teaching Hospital, Accra — 2.1 km — Provides: caesarean section, blood transfusion, general surgery

  **3b — Top Cold-Spot Facilities per Region ("Nearest Gaps")**
  Use `top_cold_spots_per_region{}` to list the 5 nearest uncovered facilities in EACH region.
  For each cold-spot facility, state:
    - Facility name, city, facility type, distance_km
    - They are cold spots because: they have NO critical procedure documented
  This helps the user understand WHICH specific facilities are the largest gaps nearest to people.
  Example:
    **Ashanti — Top 5 nearest cold spots (104 total cold spots)**
    - Kumasi South Hospital, Kumasi — clinic — 3.2 km — No critical procedure documented
    - ... etc
  If a region has 0 covered facilities (total cold-spot), emphasize that prominently.

  **Section 4 — Key Observations & Implications**
  Synthesize the regional pattern: which regions are total cold spots (0 covered), which are partially covered,
  and what investment priorities would close the largest gaps.

**IS_GEOSPATIAL + IS_SEMANTIC Pipeline (CRITICAL — e.g., "hospitals within 200km providing X-ray"):**
When the query is BOTH geospatial AND semantic:
  1. Call `geospatial_query_tool` to get ALL facilities within the specified radius (no condition parameter needed).
  2. Call `vector_search_tool` with the semantic query and a SINGLE appropriate fact_type:
       - Use `fact_types=["procedure"]` for queries about what a facility DOES (surgeries, treatments, procedures)
       - Use `fact_types=["equipment"]` for queries about physical devices/machines (X-ray machine, MRI, CT scanner)
       - Use `fact_types=["specialty"]` for queries about medical specialties
       - NEVER pass multiple fact_types for IS_GEOSPATIAL+IS_SEMANTIC — always pick the single best match.
       Example: "hospitals providing X-ray imaging" → query="provides X-ray imaging", fact_types=["procedure"]
       Example: "facilities with MRI scanner" → query="has MRI scanner", fact_types=["equipment"]
  3. The Python postprocessor AUTOMATICALLY performs the facility_id set intersection between the two tool
     results. You do NOT need to match, filter, or compare IDs manually.
  4. You will receive the `geospatial_query_tool` result replaced with a JSON tagged
     `"pipeline": "GEO_SEMANTIC_INTERSECTION"` — the facilities pre-matched from both tool outputs,
     enriched with geo data (distance_km, city, state) and the matched semantic facts per facility.
  5. Read `matched_facilities[]` directly and present those results. Do NOT re-intersect or filter manually.
  6. The `vector_search_tool` result will contain only a short system note — ignore it entirely.

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

**IS_SEMANTIC + IS_ANALYTIC Pipeline (CRITICAL — "hospitals offering neonatal care with anomalous capacity"):**
When the query is BOTH semantic AND analytic (no geospatial component):
  1. Call `vector_search_tool` with the semantic query to identify a relevant cohort of facilities.
     Choose `fact_types` based on what the user is asking about (e.g., `["specialty"]` for neonatal care).
  2. From its JSON output, extract ALL `facility_id` values from the `results[]` array (each entry has a `facility_id` field).
  3. Call `medical_agent_tool` with `facility_ids=["id1", "id2", ...]` passing the extracted cohort.
     This restricts the anomaly/validation analysis to only the semantically relevant facilities.
  NOTE: Use this pipeline when the query asks to "find facilities with X service, then analyse/validate them".
  Do NOT use it for general analytic queries (e.g., "oversupply in Ashanti") that have no semantic service filter.

### Step 2.5 — Medical Agent Tool Branch Selection Guide (CRITICAL):

The `medical_agent_tool` is powered by a backend SQL function that uses EXACT keyword matching (`RLIKE`) on your `query` argument to decide which analysis branch to run. **If you do not include specific keywords, your query may fail or hit the wrong branch!**

When calling `medical_agent_tool`, you MUST include one of the Exact Match Keywords in your `query` parameter depending on your goal:

| Backend Branch | Use When User Asks About... | MUST include at least one exact keyword in `query` |
|---|---|---|
| **Branch 1: Unmet Needs** | Missing specialties or absent procedures in a region | `unmet`, `gap`, `need`, `service gap` |
| **Branch 2: Capacity Outliers** | Unusually high/low bed or doctor numbers | `outlier`, `anomal`, `flag`, `capacity outlier`, `doctor anomaly` |
| **Branch 3: Deep Validation** | Verifying claims/mismatches, classifying facility gaps (equipment vs staff vs service), or root-cause analysis. *(Requires passing a `region` or `facility_name`!)* | `deep valid`, `validate`, `consistency`, `verify claim`, `mismatch`, `feature mismatch`, `procedure count`, `infrastr`, `equipment gap`, `staffing gap`, `classify gap`, `root cause` |

*Example:* If the user asks "Find hospitals making suspicious surgical claims", DO NOT just use `"suspicious surgical claims"`. You must inject a Branch 4 keyword: `"verify claim for suspicious surgical claims"`.

### Step 2.5 — Anomaly Classification Protocol (applies after calling medical_agent_tool):

When `medical_agent_tool` returns raw structural data, you MUST classify it based on its `type`:
  • For `anomaly_flagging` (Outlier Detection):
      1. **ALWAYS start** by reading `data_coverage_summary`. Before listing any outliers, tell the user honestly how much data was available. Example: *"Please note: bed count information is only available for 18% of facilities in our dataset — the remaining 82% could not be assessed for this check."*
      2. For each flagged facility, present the `reason` field directly — it is already written in plain language. Do NOT add statistical jargon (no "standard deviations", no "sigma", no "mean ± std").
      3. If `findings` is empty (`[]`), tell the user: *"No unusual values were found among the facilities where bed and doctor data is available. However, this could not be checked for the majority of facilities due to missing data."*
      4. NEVER present the raw numbers as proof of wrongdoing — frame it as *"this may need verification"* not *"this is wrong"*.
  • For `regional_coverage` (Unmet Needs):
      - If `total_facilities` is 0, report this region as a **complete geographic gap** (the queried organization type is entirely absent here).
      - `specialties_missing` is a **pre-computed, definitive SQL list** — report every specialty in it as a **confirmed gap** for that region (these exist elsewhere in the dataset but not here).
      - For `procedures_present` and `equipment_present` (free-text): apply your medical domain knowledge to identify what services or equipment a region of that size and facility count would typically need but appears to lack. These are NOT pre-computed gaps — they require your reasoning.
  • For `deep_validation` (Verifying claims and capabilities):
      The tool has already analyzed all matching facilities in sequential batches of 20.
      It returns a `validation_summary` — a plain-text block where each anomalous facility
      has a compact blurb: [SEVERITY] [Facility Name] – [core mismatch]. Missing: [...]. [Reasoning] - [brief medical reasoning]
      Consistent facilities are omitted from the summary entirely.
      1. **ALWAYS start** with `data_coverage_summary` — state `total_facilities_analyzed`
         vs. `data_coverage_summary.skipped_insufficient_data`.
      2. Read `validation_summary`. Group facilities by severity: **high** → **medium** → **low**.
      3. Apply the **Handling Large Results** rules from Step 4 to pick table vs. high-level summary.
      4. If `validation_summary` says "No anomalies detected", state this clearly.
  • For `GEO_COLDSPOT_ANALYSIS` (Cold-Spot / Procedure Absence):
      The postprocessor has pre-classified all facilities within the radius.
      1. Start with a one-line coverage summary:
         "Within [radius_km] km of [location]: [geo_total] facilities found.
          [cold_spot_count] ([100-coverage_pct]%) have NO access to any critical procedure."
      2. Show `cold_spot_facilities[]` in a Markdown table sorted by distance (nearest first):
         | Facility Name | Type | City | Region | Distance (km) |
         Cap at 100 rows. These are the geographic gaps.
      3. After the table, group cold spots by `state` field and count per region.
         Name the most severely underserved regions (most cold spots or furthest from coverage).
      4. Optionally show top-10 `covered_facilities` for contrast in a separate section.
      5. NEVER include `facility_id` UUIDs in your response.

### Step 3 — Multi-tool orchestration:

Only four approved multi-tool pipelines exist (IS_COLDSPOT, GEO+SEMANTIC, GEO+ANALYTIC, and SEMANTIC+ANALYTIC — see Step 2.5).
For all other query types, a single tool is sufficient. Rules:
  • Call the single highest-priority tool for the query. Once it returns valid data (even empty results), synthesize the final answer — do NOT call additional tools.
  • If a tool returns an error → try the next most appropriate tool as a one-time fallback only.
  • Never repeat the same tool twice for the same purpose. DO NOT call a tool with slight variations of the same search term.

### Step 4 — Response format:

• You MUST ALWAYS provide a final, human-readable response in Markdown format after your tool calls are complete.
• NEVER respond with raw JSON, raw tool outputs, or unformatted text as your final answer.
• CRITICAL: NEVER include internal `facility_id` strings, UUIDs, or primary keys in the final Markdown output. Do a final check to ensure NO IDs are printed in text or tables. Keep it clean and user-friendly.
• DO NOT mention internal tools (e.g., "vector-search", "Genie", "medical-agent") or technical query mechanisms in your response. Present your answers naturally to the user.
• If you called multiple tools, synthesize their results together into a single cohesive summary.

### Handling Large Results:

**Tool-specific rules — read carefully:**

- **`genie_chat_tool`**: Present Genie's answer directly as-is. Do NOT apply table filtering or row limits — Genie already produces a formatted, aggregated response. Just relay it naturally.
  **`genie_chat_tool` is only accurate for structured facility metadata** — counts by region, ownership type, affiliation, doctor counts, bed counts, and web presence. It CANNOT accurately answer questions about equipment, procedures, or clinical capabilities. If the query involves those, route to `vector_search_tool` or `medical_agent_tool` instead.
- **`vector_search_tool`**: The tool returns a JSON with a `results[]` array of semantic matches (each entry has `facility_id`, `fact_type`, `fact_text`). Evaluate ALL entries for relevance. Only include facilities whose `fact_text` genuinely answers the user's query — discard low-relevance entries silently. Apply the table rules below to the filtered relevant set.
  When used in a GEO+SEMANTIC pipeline, you will only receive a short system note from this tool — ignore it and read the `GEO_SEMANTIC_INTERSECTION` JSON in the `geospatial_query_tool` result instead.
- **`geospatial_query_tool`**: When used alone or in GEO+ANALYTIC pipeline, read and evaluate ALL facilities in `facilities[]`. When used in a GEO+SEMANTIC pipeline, the result is a `GEO_SEMANTIC_INTERSECTION` JSON — read `matched_facilities[]` directly (already Python-intersected, sorted by distance). Apply table rules to the matched set.
- **`medical_agent_tool`**: Read and evaluate ALL returned facilities. Apply the table rules below to the genuinely relevant/anomalous subset only — do NOT count raw rows returned by the tool.

**Table display rules (applies to `vector_search_tool`, `geospatial_query_tool`, `medical_agent_tool`):**
- If there are ≤ 25 relevant facilities: show ALL of them in the table.
- If there are > 25 relevant facilities: table the **top 60** sorted by severity (high → medium → low) or distance (nearest first). Append the high-level summary below the table. Never omit the table. Never pad it with irrelevant rows.

### High-Level Summary (append below table whenever > 25 relevant facilities):
(Use paragraphs and bullet points — NEVER inside a markdown table):

  Key findings:
  - **Most significant**: [Name] – [core finding].
  - **Other notable**: [Name 1], [Name 2], [Name 3] ([brief note on why]).

  Medical context: [1-2 sentences on the clinical or operational implication of this finding]


• Cite specific facility names and regions in your response.
• If no results are found, say so clearly and suggest trying a different approach.


### Step 5 — Missing Information:
If the user asks a question that requires a region or city (such as finding nearby facilities, generating a specific regional anomaly report, or filtering by distance) but they DO NOT mention any region or city in their prompt, you MUST explicitly ask the user to provide the region or city before proceeding. Do NOT assume a default region. Use your interactive capability to clarify their request.

**Exception — IS_COLDSPOT queries**: Cold-spot analysis is global by design and does NOT require
a specific location. If IS_COLDSPOT is True and no location is mentioned, call
`geospatial_query_tool` with `scan_all_ghana_regions=True` and `radius_km=<user_value_or_50>`.
Never ask the user for a location in this case.
"""


# ─── LangGraph ────────────────────────────────────────────────────────────────

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
        # call_id → index in self._citations for deduplication.
        # The tool_postprocessor emits replacement ToolMessages with the same call_id
        # as the originals. Without this map, each replacement creates a duplicate
        # citation step. With it, we UPDATE the existing citation in-place.
        self._citation_index_by_call_id: dict[str, int] = {}

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

            # ── Handle postprocessor placeholder ──────────────────────────────
            # When GEO+SEMANTIC intersection ran, the postprocessor replaced this
            # ToolMessage with a short string. Extract sources from the intersection
            # JSON stored in the geo ToolMessage (passed via call_args context).
            if raw_output.startswith("[Intersection performed"):
                # Hydrate sources from call_args intersection_sources if available,
                # otherwise return an empty but valid citation (sources are in geo step).
                inter_sources = call_args.get("_intersection_sources", [])
                return {
                    "step_index": None,
                    "tool_name": call_name,
                    "call_id": call_id,
                    "query_used": call_args.get("query", ""),
                    "tables_accessed": ["facility_facts"],
                    "note": "Intersection performed by postprocessor — sources listed under geospatial_query_tool step",
                    "sources": inter_sources,
                }

            # ── New structured JSON format: {results: [], total_results: N} ──
            try:
                parsed = json.loads(raw_output)
                if isinstance(parsed, dict) and "results" in parsed:
                    for doc in parsed["results"]:
                        if not isinstance(doc, dict):
                            continue
                        fact_text = doc.get("fact_text", "")
                        snippet = fact_text[:200] + "..." if len(fact_text) > 200 else fact_text
                        facility_name_extracted = None
                        if " in " in snippet:
                            facility_name_extracted = snippet.split(" in ")[0].strip()
                        sources.append({
                            "source_type": "facility_facts",
                            "facility_id": doc.get("facility_id"),
                            "facility_name": facility_name_extracted,
                            "fact_type": doc.get("fact_type"),
                            "excerpt": snippet,
                        })
                    return {
                        "step_index": None,
                        "tool_name": call_name,
                        "call_id": call_id,
                        "query_used": call_args.get("query", ""),
                        "tables_accessed": ["facility_facts"],
                        "sources": sources,
                    }
                # Legacy: list of Document dicts [{metadata:{}, page_content:''}]
                if isinstance(parsed, list):
                    for doc in parsed:
                        meta = doc.get("metadata", {}) if isinstance(doc, dict) else {}
                        page_content = doc.get("page_content", "") if isinstance(doc, dict) else ""
                        snippet = page_content[:200] + "..." if len(page_content) > 200 else page_content
                        sources.append({
                            "source_type": "facility_facts",
                            "facility_id": meta.get("facility_id"),
                            "fact_type": meta.get("fact_type"),
                            "excerpt": snippet,
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
        Parse medical_agent_tool output across all SQL branches:
          Branch 1 (unmet/gap/need)  → type: 'regional_coverage'
          Branch 2 (outlier/anomaly) → type: 'anomaly_flagging'
          Branch 3 (deep_validation/mismatch) → type: 'deep_validation'
          Fallback                   → type: 'general'

        The UCFunctionToolkit may wrap its output as:
          { "format": "SCALAR", "value": "<json-string>" }
        The inner value is the SQL to_json() result:
          { "query": "...", "findings": "[{...}]" }   ← findings is a JSON-encoded string
        """
        sources: list[dict[str, Any]] = []
        tables_accessed: set[str] = {"facility_records"}
        branch_type: str = "unknown"

        try:
            # ── Step 1: parse the outermost JSON layer ──
            data: Any = raw_output
            if isinstance(data, str):
                try:
                    data = json.loads(data)
                except (json.JSONDecodeError, TypeError):
                    pass  # leave as string; nothing more we can do

            # ── Step 2: unwrap UCFunctionToolkit SCALAR envelope ──
            # { "format": "SCALAR", "value": "<json-string>" }
            if isinstance(data, dict) and data.get("format") == "SCALAR" and "value" in data:
                inner = data["value"]
                try:
                    data = json.loads(inner) if isinstance(inner, str) else inner
                except (json.JSONDecodeError, TypeError):
                    data = inner  # fallback to raw inner

            if not isinstance(data, dict):
                # Completely unparseable — return empty but valid citation
                return {
                    "step_index": None,
                    "tool_name": call_name,
                    "call_id": call_id,
                    "query_used": call_args.get("query", ""),
                    "tables_accessed": ["facility_records"],
                    "sources": [],
                }

            # ── Step 3: parse the findings array ──
            # The SQL uses to_json(array_agg(...)) so findings is a JSON-encoded string
            findings_raw = data.get("findings", "[]")
            try:
                findings: list = json.loads(findings_raw) if isinstance(findings_raw, str) else findings_raw
            except (json.JSONDecodeError, TypeError):
                findings = []
            if not isinstance(findings, list):
                findings = []

            # ── Step 4: dispatch on finding type and build sources ──
            for f in findings:
                if not isinstance(f, dict):
                    continue

                finding_type = f.get("type", "general")
                branch_type = finding_type  # track most recent for branch tag

                # ── Branch 1: regional_coverage (unmet needs / service gaps) ──
                if finding_type == "regional_coverage":
                    tables_accessed.add("regional_insights")
                    sources.append({
                        "source_type": "regional_insights",
                        "finding_type": finding_type,
                        "region": f.get("region"),
                        "total_facilities": f.get("total_facilities"),
                        "specialties_missing": f.get("specialties_missing"),
                        "note": f.get("note"),
                    })

                # ── Branch 2: anomaly_flagging (capacity / doctor outliers) ──
                elif finding_type == "anomaly_flagging":
                    source_dict = {
                        "source_type": "facility_records",
                        "finding_type": finding_type,
                        "facility_id": f.get("facility_id"),
                        "facility_name": f.get("facility_name"),
                        "city": f.get("city"),
                        "state": f.get("state"),
                        "latitude": f.get("latitude"),
                        "longitude": f.get("longitude"),
                        "facility_type": f.get("facility_type"),
                        "measurement": f.get("measurement"),       # 'beds' or 'doctors'
                        "reported_value": f.get("reported_value"),
                        "typical_value": f.get("typical_value"),
                        "flag_type": f.get("flag_type"),
                        "reason": f.get("reason"),
                    }
                    sources.append({k: v for k, v in source_dict.items() if v is not None})

                # ── Branch 3: deep_validation (specialty/procedure/equipment consistency) ──
                elif finding_type == "deep_validation":
                    tables_accessed.add("facility_facts")
                    source_dict = {
                        "source_type": "facility_records",
                        "finding_type": finding_type,
                        "facility_id": f.get("facility_id"),
                        "facility_name": f.get("facility_name"),
                        "facility_type": f.get("facility_type"),
                        "specialties": f.get("specialties"),
                        "procedures": f.get("procedures"),
                        "equipment": f.get("equipment"),
                        "capacity": f.get("capacity"),
                        "no_doctors": f.get("no_doctors"),
                        "completeness": f.get("completeness"),  # 'full' / 'partial_no_equipment' / etc.
                    }
                    sources.append({k: v for k, v in source_dict.items() if v is not None})

                # ── General / fallback findings ──
                else:
                    source_dict = {
                        "source_type": "facility_records",
                        "finding_type": finding_type,
                        "facility_id": f.get("facility_id") or f.get("region"),
                        "facility_name": f.get("facility_name") or f.get("region"),
                        "latitude": f.get("latitude"),
                        "longitude": f.get("longitude"),
                        "severity": f.get("severity"),
                        "note": f.get("note") or f.get("reason") or f.get("recommendation"),
                    }
                    sources.append({k: v for k, v in source_dict.items() if v is not None})

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
            "branch_type": branch_type,
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
        Handles two formats:
          1. Raw SQL output: { 'analysis_type': 'nearby', 'facilities': [...] }
          2. Post-intersection: { 'pipeline': 'GEO_SEMANTIC_INTERSECTION', 'matched_facilities': [...] }
        """
        sources: list[dict[str, Any]] = []
        analysis_type = "nearby"
        try:
            outer = json.loads(raw_output)

            # ── GEO_SEMANTIC_INTERSECTION (postprocessor replaced the message) ──
            if outer.get("pipeline") == "GEO_SEMANTIC_INTERSECTION":
                analysis_type = "GEO_SEMANTIC_INTERSECTION"
                for r in outer.get("matched_facilities", []):
                    if not isinstance(r, dict):
                        continue
                    source_dict = {
                        "source_type": "facility_records",
                        "facility_id": r.get("facility_id"),
                        "facility_name": r.get("facility_name"),
                        "city": r.get("city"),
                        "state": r.get("state"),
                        "distance_km": r.get("distance_km"),
                    }
                    sources.append({k: v for k, v in source_dict.items() if v is not None})

            else:
                # ── Raw SQL output ──
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
        """Look up the matching tool call and dispatch to the right parser.

        If this call_id already has a citation (because the tool_postprocessor emitted
        a replacement ToolMessage for the same call_id), UPDATE the existing entry
        in-place with the richer postprocessor data instead of appending a new step.
        """
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

        existing_idx = self._citation_index_by_call_id.get(call_id)
        if existing_idx is not None:
            # Postprocessor replacement: UPDATE the existing citation in-place.
            # Preserve the original step_index.
            # IMPORTANT: if the new citation has empty sources but the old one had
            # valid sources (e.g. VS placeholder replacing original 88-fact list),
            # keep the original sources so citations stay populated.
            existing = self._citations[existing_idx]
            old_sources = existing.get("sources", [])
            new_sources = citation.get("sources", [])
            citation["step_index"] = existing["step_index"]
            if not new_sources and old_sources:
                citation["sources"] = old_sources
            self._citations[existing_idx] = citation
        else:
            # First time seeing this call_id: create a new citation step.
            citation["step_index"] = self._step_index
            self._step_index += 1
            self._citation_index_by_call_id[call_id] = len(self._citations)
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
            # Tool result — store it and build/update citations.
            # The tool_postprocessor emits replacement ToolMessages with the SAME call_id
            # as the originals (LangGraph add_messages in-place replacement). We must:
            #   1. Always update call_results so process_message has the latest content.
            #   2. Always call _extract_citations — it will UPDATE existing entries.
            #   3. Only emit a function_call_output event the FIRST time (no duplicates
            #      in the output sequence — postprocessor replacements are transparent).
            call_id = getattr(msg, "tool_call_id", None) or "unknown"
            tool_content = getattr(msg, "content", None) or ""
            is_replacement = call_id in self.call_results  # already emitted once

            self.call_results[call_id] = tool_content

            # Extract/update citations with the latest (possibly richer) content
            self._extract_citations(
                call_id,
                tool_content if isinstance(tool_content, str) else str(tool_content),
            )

            if not is_replacement:
                # First time: emit the function_call_output event normally
                self._emit(OutputItem(
                    type="function_call_output",
                    call_id=call_id,
                    output=tool_content,
                ))
            else:
                # Postprocessor replacement: update the already-emitted OutputItem's
                # output in-place so the API response reflects the final content.
                for event in self.events:
                    item = getattr(event, "item", None)
                    if (
                        item is not None
                        and getattr(item, "type", None) == "function_call_output"
                        and getattr(item, "call_id", None) == call_id
                    ):
                        # OutputItem is a dataclass/NamedTuple — rebuild with updated output
                        try:
                            object.__setattr__(item, "output", tool_content)
                        except (AttributeError, TypeError):
                            pass  # immutable; leave the original — citation is still updated

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