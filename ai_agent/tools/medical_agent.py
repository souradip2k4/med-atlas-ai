# ─── Tool 3 — Medical Agent ───────────────────────────────────────────────────

# Batch size for deep validation LLM calls
from langchain_core.messages import HumanMessage
from ai_agent.config import LLM_ENDPOINT
from databricks_langchain import ChatDatabricks
from flask import json
from ai_agent.config import ANALYZE_UC_FUNCTION_NAME
from unitycatalog.ai.langchain.toolkit import UCFunctionToolkit
from langchain.tools import tool
import mlflow
import json

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
