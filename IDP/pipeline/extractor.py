"""
extractor.py — LLM validation pipeline (Step 2 only).

After deduplication, each consolidated facility row is passed through a single
LLM call that:
  1. Cleans the facility name (strips address junk from name variants)
  2. Validates medical arrays (keeps valid items, removes noise, returns null if empty)
  3. Generates a description only when the CSV has none and enough clinical data exists
  4. Extracts capacity + noDocors integers from the cleaned data

Step 1 (org classification) has been removed — organization_type is taken directly from CSV.
Steps 3 & 4 (specialty extraction, structured facility info) remain skipped — CSV data used.
"""

import os
import re
import logging
from typing import Any, Dict, Optional

from dotenv import load_dotenv
from langchain_core.messages import SystemMessage, HumanMessage
from databricks_langchain import ChatDatabricks

from config.free_form import (
    FacilityFacts,
    FREE_FORM_SYSTEM_PROMPT,
)
from pipeline.preprocessor import (
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
    return ``response.content`` as a list of content-block dicts.
    Reasoning/thinking blocks are skipped.
    """
    if isinstance(text, list):
        parts = []
        for block in text:
            if isinstance(block, dict):
                block_type = block.get("type", "text")
                if block_type in ("reasoning", "thinking"):
                    continue
                parts.append(block.get("text") or block.get("content") or "")
            else:
                parts.append(str(block))
        text = "".join(parts)

    text = text.strip()
    text = re.sub(r"^```(?:json)?\s*", "", text)
    text = re.sub(r"\s*```$", "", text)
    return text.strip()


class LLMExtractor:
    """
    Runs a single LLM validation pass against each consolidated facility row.

    The LLM:
      - Cleans the facility name
      - Validates medical arrays (removes noise, returns null if all items invalid)
      - Generates a description when absent and clinical data is sufficient
      - Extracts capacity + noDocors integers
    """

    def __init__(self, endpoint: str | None = None):
        self.endpoint = endpoint or os.getenv(
            "LLM_ENDPOINT", "databricks-meta-llama-3-1-70b-instruct"
        )
        self.llm = ChatDatabricks(
            endpoint=self.endpoint,
            temperature=0.2,
            max_tokens=4096,
        )

    # ── Internal call helpers ────────────────────────────────────────

    def _call_llm(self, system_prompt: str, user_text: str, max_retries: int = 2) -> str:
        """Invoke the LLM and return the raw response string."""
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
            return model_cls.model_validate_json(raw_json)
        except Exception as e:
            logger.error(
                "Validation failed for %s: %s — skipping",
                model_cls.__name__, e,
            )
            return None

    # ── Step 2: Facility validation + cleaning ───────────────────────

    def validate_facility_data(
        self, text: str, facility_name: str, existing_description: str | None = None
    ) -> Optional[FacilityFacts]:
        """Run the LLM validation pass on the consolidated facility data."""
        if existing_description and existing_description.strip():
            desc_note = (
                "- IMPORTANT: A description already exists for this facility. "
                "You MUST return `null` for the description field. Do not rephrase or improve it."
            )
        else:
            desc_note = ""

        prompt = (
            FREE_FORM_SYSTEM_PROMPT
            .replace("{organization}", facility_name)
            .replace("{existing_description_note}", desc_note)
        )
        raw = self._call_llm(prompt, text)
        return self._parse(FacilityFacts, raw)

    # ── Full row processing ──────────────────────────────────────────

    def process_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """
        Run the single-step LLM validation chain on one consolidated facility row.

        Step 1 (org classification) is REMOVED — organization_type comes from CSV.
        Steps 3 & 4 remain skipped — CSV data used directly in merger.py.

        Returns a dict with:
          - facts_output  : FacilityFacts (validated/cleaned by LLM)
          - facility_name : primary name from row (shortest variant from deduplicator)
          - synthesized_text, source_row_id
        """
        source_row_id = str(row.get("unique_id") or row.get("pk_unique_id") or "")

        # Primary name = shortest variant chosen by deduplicator
        facility_name = row.get("name") or "Unknown Facility"

        # Build the validation context text for the LLM
        fact_text = synthesize_for_fact_extraction(row)

        # Pass existing description so the LLM skips description generation when present
        existing_description = row.get("description") or None
        facts_output = self.validate_facility_data(fact_text, facility_name, existing_description)

        return {
            "org_output": None,           # Step 1 removed
            "facts_output": facts_output,
            "specialties_output": None,   # CSV specialties used directly
            "facility_output": None,      # CSV structured fields used directly
            "facility_name": facility_name,
            "synthesized_text": fact_text,
            "source_row_id": source_row_id,
        }
