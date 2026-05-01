from mlflow.types.responses import (
    ResponsesAgentStreamEvent,
    ResponseOutputItemDoneEvent,
    OutputItem,
)
import uuid
from mlflow.types.responses_helpers import (
    Content
)

from typing import  Any
import re
import json

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
