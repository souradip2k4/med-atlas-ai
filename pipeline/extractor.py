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
from pipeline.preprocessor import synthesize_row_text

load_dotenv()
logger = logging.getLogger(__name__)

# ── JSON enforcement suffix ──────────────────────────────────────────────
_JSON_SUFFIX = (
    "\n\nIMPORTANT: Return ONLY valid JSON. "
    "No explanations, no markdown fences, no extra text."
)


def _strip_markdown_json(text: str) -> str:
    """Remove ```json ... ``` wrappers if the LLM added them."""
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
        from langchain_databricks import ChatDatabricks

        self.endpoint = endpoint or os.getenv(
            "LLM_ENDPOINT", "databricks-meta-llama-3-1-70b-instruct"
        )
        self.llm = ChatDatabricks(
            endpoint=self.endpoint,
            temperature=0.0,
            max_tokens=4096,
        )

    # ── Internal call helpers ────────────────────────────────────────

    def _call_llm(self, system_prompt: str, user_text: str) -> str:
        """Invoke the LLM and return the raw response string."""
        messages = [
            SystemMessage(content=system_prompt + _JSON_SUFFIX),
            HumanMessage(content=user_text),
        ]
        response = self.llm.invoke(messages)
        return _strip_markdown_json(response.content)

    def _parse_with_retry(self, model_cls, raw_json: str, system_prompt: str, user_text: str):
        """
        Parse ``raw_json`` via ``model_cls.model_validate_json``.
        Retry the LLM call once on failure.
        Returns (parsed_model, confidence) or (None, 0.0).
        """
        # Attempt 1
        try:
            parsed = model_cls.model_validate_json(raw_json)
            return parsed, 0.85
        except Exception as e1:
            logger.warning(
                "Validation failed (attempt 1) for %s: %s — retrying",
                model_cls.__name__, e1,
            )

        # Retry — call LLM again
        try:
            raw_json_retry = self._call_llm(system_prompt, user_text)
            parsed = model_cls.model_validate_json(raw_json_retry)
            return parsed, 0.65  # lower confidence on retry
        except Exception as e2:
            logger.error(
                "Validation failed (attempt 2) for %s: %s — skipping",
                model_cls.__name__, e2,
            )
            return None, 0.0

    # ── Step 1: Organization extraction ──────────────────────────────

    def extract_organizations(
        self, text: str
    ) -> Tuple[Optional[OrganizationExtractionOutput], float]:
        prompt = ORGANIZATION_EXTRACTION_SYSTEM_PROMPT
        raw = self._call_llm(prompt, text)
        return self._parse_with_retry(OrganizationExtractionOutput, raw, prompt, text)

    # ── Step 2: Facility fact extraction ─────────────────────────────

    def extract_facility_facts(
        self, text: str, facility_name: str
    ) -> Tuple[Optional[FacilityFacts], float]:
        prompt = FREE_FORM_SYSTEM_PROMPT.replace("{organization}", facility_name)
        raw = self._call_llm(prompt, text)
        return self._parse_with_retry(FacilityFacts, raw, prompt, text)

    # ── Step 3: Medical specialty extraction ──────────────────────────

    def extract_medical_specialties(
        self, text: str, facility_name: str
    ) -> Tuple[Optional[MedicalSpecialties], float]:
        prompt = MEDICAL_SPECIALTIES_SYSTEM_PROMPT.replace("{organization}", facility_name)
        raw = self._call_llm(prompt, text)
        return self._parse_with_retry(MedicalSpecialties, raw, prompt, text)

    # ── Step 4: Facility structured extraction ───────────────────────

    def extract_facility_info(
        self, text: str, facility_name: str
    ) -> Tuple[Optional[Facility], float]:
        prompt = ORGANIZATION_INFORMATION_SYSTEM_PROMPT.replace("{organization}", facility_name)
        raw = self._call_llm(prompt, text)
        return self._parse_with_retry(Facility, raw, prompt, text)

    # ── Full row processing ──────────────────────────────────────────

    def process_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """
        Run the complete 4-step extraction chain on a single row.

        Returns a dict with:
          - org_output, facts_output, specialties_output, facility_output
          - synthesized_text, source_row_id
          - per-step confidence scores
        """
        # Synthesise row text
        synth_text = synthesize_row_text(row)
        source_row_id = str(row.get("unique_id") or row.get("pk_unique_id") or "")

        # Step 1 — Organizations
        org_output, conf_org = self.extract_organizations(synth_text)

        # Determine primary facility name
        facility_name = None
        if org_output and org_output.facilities:
            facility_name = org_output.facilities[0]
        if not facility_name:
            facility_name = row.get("name") or "Unknown Facility"

        # Step 2 — Facility facts
        facts_output, conf_facts = self.extract_facility_facts(synth_text, facility_name)

        # Step 3 — Medical specialties
        specialties_output, conf_spec = self.extract_medical_specialties(synth_text, facility_name)

        # Step 4 — Facility structured info
        facility_output, conf_fac = self.extract_facility_info(synth_text, facility_name)

        return {
            "org_output": org_output,
            "facts_output": facts_output,
            "specialties_output": specialties_output,
            "facility_output": facility_output,
            "facility_name": facility_name,
            "synthesized_text": synth_text,
            "source_row_id": source_row_id,
            "confidence_org": conf_org,
            "confidence_facts": conf_facts,
            "confidence_specialties": conf_spec,
            "confidence_facility": conf_fac,
        }
