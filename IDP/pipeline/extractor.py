"""
extractor.py — 4-step LLM extraction pipeline.

Steps:
  1. Organization extraction   → OrganizationExtractionOutput
  2. Facility fact extraction   → FacilityFacts
  3. Medical specialty extraction → MedicalSpecialties
  4. Facility structured info   → Facility

Uses ChatDatabricks from langchain-databricks + Pydantic strict validation.
"""

import os
import re
import logging
from typing import Any, Dict, Optional, Tuple

from dotenv import load_dotenv
from langchain_core.messages import SystemMessage, HumanMessage
from databricks_langchain import ChatDatabricks

from config.organization_extraction import (
    OrganizationExtractionOutput,
    ORGANIZATION_EXTRACTION_SYSTEM_PROMPT,
)
from config.free_form import (
    FacilityFacts,
    FREE_FORM_SYSTEM_PROMPT,
)
from config.medical_specialties import (
    MedicalSpecialties,
    MEDICAL_SPECIALTIES_SYSTEM_PROMPT,
)
from config.facility_and_ngo_fields import (
    Facility,
    ORGANIZATION_INFORMATION_SYSTEM_PROMPT,
)
from pipeline.preprocessor import (
    synthesize_row_text,
    synthesize_for_org_classification,
    synthesize_for_fact_extraction,
)

load_dotenv()
logger = logging.getLogger(__name__)

# ── JSON enforcement suffix ──────────────────────────────────────────────
_JSON_SUFFIX = (
    "\n\nIMPORTANT: Return ONLY valid JSON. "
    "No explanations, no markdown fences, no extra text."
)


def _strip_markdown_json(text: str | list) -> str:
    """Remove ```json ... ``` wrappers if the LLM added them.

    Also handles the case where newer versions of databricks_langchain
    return ``response.content`` as a list of content-block dicts,
    e.g. ``[{"type": "text", "text": "..."}]``.

    Reasoning/thinking blocks (``{"type": "reasoning", ...}``) are
    explicitly skipped — only ``{"type": "text", ...}`` blocks are
    included so the chain-of-thought prefix does not corrupt the JSON.
    """
    # Normalise list-of-blocks to a plain string
    if isinstance(text, list):
        parts = []
        for block in text:
            if isinstance(block, dict):
                block_type = block.get("type", "text")
                # Skip reasoning / thinking blocks entirely
                if block_type in ("reasoning", "thinking"):
                    continue
                parts.append(block.get("text") or block.get("content") or "")
            else:
                parts.append(str(block))
        text = "".join(parts)

    text = text.strip()
    # Remove ```json or ``` fences
    text = re.sub(r"^```(?:json)?\s*", "", text)
    text = re.sub(r"\s*```$", "", text)
    return text.strip()


class LLMExtractor:
    """
    Runs the 4-step LLM extraction chain against a single row.

    Each step:
      1. Builds system prompt + human message
      2. Calls the LLM
      3. Parses output with Pydantic ``model_validate_json``
      4. Retries once on validation failure; logs + skips on second failure
    """

    def __init__(self, endpoint: str | None = None):

        self.endpoint = endpoint or os.getenv(
            "LLM_ENDPOINT", "databricks-meta-llama-3-1-70b-instruct"
        )
        self.llm = ChatDatabricks(
            endpoint=self.endpoint,
            temperature=0.0,
            max_tokens=4096,
        )

    # ── Internal call helpers ────────────────────────────────────────

    def _call_llm(self, system_prompt: str, user_text: str, max_retries: int = 2) -> str:
        """Invoke the LLM and return the raw response string.

        Retries up to ``max_retries`` times if the LLM returns an empty
        response (e.g. a reasoning-model that emitted only a thinking block
        with no text block on first attempt).
        """
        messages = [
            SystemMessage(content=system_prompt + _JSON_SUFFIX),
            HumanMessage(content=user_text),
        ]
        for attempt in range(1, max_retries + 1):
            response = self.llm.invoke(messages)
            result = _strip_markdown_json(response.content)
            if result:
                return result
            logger.warning(
                "LLM returned empty response (attempt %d/%d) — retrying",
                attempt, max_retries,
            )
        logger.error("LLM returned empty response after %d attempts", max_retries)
        return ""

    def _parse(self, model_cls, raw_json: str):
        """Parse ``raw_json`` via ``model_cls.model_validate_json``."""
        if not raw_json or not raw_json.strip():
            logger.warning(
                "Empty JSON string passed to parser for %s — skipping",
                model_cls.__name__,
            )
            return None
        try:
            parsed = model_cls.model_validate_json(raw_json)
            return parsed
        except Exception as e:
            logger.error(
                "Validation failed for %s: %s — skipping",
                model_cls.__name__, e,
            )
            return None

    # ── Step 1: Organization extraction ──────────────────────────────

    def extract_organizations(self, text: str) -> Optional[OrganizationExtractionOutput]:
        prompt = ORGANIZATION_EXTRACTION_SYSTEM_PROMPT
        raw = self._call_llm(prompt, text)
        return self._parse(OrganizationExtractionOutput, raw)

    # ── Step 2: Facility fact extraction ─────────────────────────────

    def extract_facility_facts(self, text: str, facility_name: str) -> Optional[FacilityFacts]:
        prompt = FREE_FORM_SYSTEM_PROMPT.replace("{organization}", facility_name)
        raw = self._call_llm(prompt, text)
        return self._parse(FacilityFacts, raw)

    # ── Step 3: Medical specialty extraction ──────────────────────────

    def extract_medical_specialties(self, text: str, facility_name: str) -> Optional[MedicalSpecialties]:
        prompt = MEDICAL_SPECIALTIES_SYSTEM_PROMPT.replace("{organization}", facility_name)
        raw = self._call_llm(prompt, text)
        return self._parse(MedicalSpecialties, raw)

    # ── Step 4: Facility structured extraction ───────────────────────

    def extract_facility_info(self, text: str, facility_name: str) -> Optional[Facility]:
        prompt = ORGANIZATION_INFORMATION_SYSTEM_PROMPT.replace("{organization}", facility_name)
        raw = self._call_llm(prompt, text)
        return self._parse(Facility, raw)

    # ── Full row processing ──────────────────────────────────────────

    def process_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """
        Run the 2-step extraction chain on a single row.

        Steps 3 (specialties) and 4 (structured info) are skipped:
          - Specialties already exist as clean camelCase enums in the CSV.
          - Structured fields (address, phone, etc.) come from the CSV directly.
          - Description is now generated by Step 2.
          - number_doctors / no_beds use regex fallback in merger.py.

        Returns a dict with:
          - org_output, facts_output
          - facility_name, synthesized_text, source_row_id
        """
        source_row_id = str(row.get("unique_id") or row.get("pk_unique_id") or "")

        # Step 1 — Organization classification (targeted text)
        org_text = synthesize_for_org_classification(row)
        org_output = self.extract_organizations(org_text)

        # Determine primary facility name
        facility_name = None
        if org_output and org_output.facilities:
            facility_name = org_output.facilities[0]
        if not facility_name:
            facility_name = row.get("name") or "Unknown Facility"

        # Step 2 — Fact extraction + description (targeted text)
        fact_text = synthesize_for_fact_extraction(row)
        facts_output = self.extract_facility_facts(fact_text, facility_name)

        # Steps 3 & 4 SKIPPED — CSV data used directly in merger.py

        return {
            "org_output": org_output,
            "facts_output": facts_output,
            "specialties_output": None,   # CSV specialties used directly
            "facility_output": None,      # CSV structured fields used directly
            "facility_name": facility_name,
            "synthesized_text": fact_text,
            "source_row_id": source_row_id,
        }
