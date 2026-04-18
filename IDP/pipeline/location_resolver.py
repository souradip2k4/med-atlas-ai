"""
location_resolver.py — Gemini-powered location inference for facilities.

Resolves missing city and/or region (state) for a facility using the
Gemini 2.0 Flash model. Called only when the deterministic dictionary
lookup fails. The full Ghana region→city mapping is loaded once from
location.json and embedded in the prompt as reference context.

Resolution cases handled:
  Case 1 — city known, region missing: Gemini predicts the region only.
  Case 2 — both city and region missing: Gemini predicts both.
"""

import json
import logging
import os
from typing import Optional

import google.generativeai as genai
from pydantic import BaseModel

logger = logging.getLogger(__name__)

# ── Load location.json once at module import time ──────────────────────────
_LOCATION_JSON_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..", "location.json"
)

def _load_location_json() -> dict:
    try:
        with open(_LOCATION_JSON_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.warning("Could not load location.json: %s", e)
        return {}

_LOCATION_DATA: dict = _load_location_json()
_LOCATION_JSON_STR: str = json.dumps(_LOCATION_DATA, indent=2, ensure_ascii=False)


# ── Pydantic model for structured output ─────────────────────────────────

import time
import threading

class StrictThrottle:
    def __init__(self, rpm: int):
        self.interval = 60.0 / rpm
        self.last_call = 0.0
        self.lock = threading.Lock()

    def consume(self):
        with self.lock:
            now = time.monotonic()
            elapsed = now - self.last_call
            if elapsed < self.interval:
                time.sleep(self.interval - elapsed)
            self.last_call = time.monotonic()

_GEMINI_LIMITER = StrictThrottle(14)  # 14 RPM to be incredibly safe with Google's window

class InferredLocation(BaseModel):
    inferred_city: Optional[str] = None
    inferred_region: Optional[str] = None


# ── System Prompt ──────────────────────────────────────────────────────────
_SYSTEM_PROMPT = """You are a Ghanaian geography expert. Your task is to infer the missing city and/or region for a healthcare facility in Ghana based on the address information provided.

You will be given:
- A Ghana region-to-cities reference dictionary
- The facility's name, address lines, and country (always Ghana)
- Whether city or region (or both) are missing

REFERENCE DICTIONARY (region → list of cities):
{location_json}

VALID REGION NAMES (you MUST use exactly one of these, or null):
{region_list}

RULES:
1. Analyze the facility name and all address fields carefully for geographic clues.
2. If region is missing but city is provided, determine which region that city belongs to using the reference dictionary or your knowledge of Ghana.
3. If both city and region are missing, infer both from the address lines using the reference dictionary and your knowledge.
4. The "region" field MUST be exactly one of the valid region names from the list above — copy it character-for-character. Do NOT append "Region", "District", or any other word. Return null if you cannot determine it with confidence.
5. The "city" field must be the bare city or town name only. Do NOT append "City", "Town", "District", "Ghana", a region name, or any other qualifier. Correct: "Accra", "Kumasi", "Cape Coast". Wrong: "Accra City", "Accra, Ghana", "Accra (Greater Accra)". Return null if you cannot determine it.
6. If you are not confident (less than 70% sure), return null for that field rather than guessing.
7. Return ONLY valid JSON — no explanations, no markdown.

OUTPUT FORMAT (strict JSON):
{{"inferred_city": "City Name or null", "inferred_region": "Region Name or null"}}"""


class GeminiLocationResolver:
    """Calls Gemini 2.0 Flash to infer missing city and/or region."""

    def __init__(self) -> None:
        api_key = os.getenv("GEMINI_API_KEY")
        if not api_key:
            raise ValueError(
                "GEMINI_API_KEY is not set in the environment. "
                "Add it to your .env file."
            )
        genai.configure(api_key=api_key)
        self._model = genai.GenerativeModel(
            model_name="gemini-3.1-flash-lite-preview",
            generation_config=genai.types.GenerationConfig(
                temperature=0.5,
                max_output_tokens=256,
            ),
        )
        self._region_list = list(_LOCATION_DATA.keys())

    def resolve_location(
        self,
        facility_name: Optional[str],
        city: Optional[str],
        address_line1: Optional[str],
        address_line2: Optional[str],
        address_line3: Optional[str],
        country: str = "Ghana",
    ) -> dict:
        """Infer missing city and/or region using Gemini.

        Returns
        -------
        dict with keys:
            inferred_city   : str | None
            inferred_region : str | None
        """
        city_missing = not city or not str(city).strip()
        region_list_str = "\n".join(f"  - {r}" for r in self._region_list)

        system_prompt = _SYSTEM_PROMPT.format(
            location_json=_LOCATION_JSON_STR,
            region_list=region_list_str,
        )

        # Build a clear user message
        parts = [
            f"Facility Name: {facility_name or 'Unknown'}",
            f"Address Line 1: {address_line1 or 'N/A'}",
            f"Address Line 2: {address_line2 or 'N/A'}",
            f"Address Line 3: {address_line3 or 'N/A'}",
            f"City: {city or 'MISSING'}",
            f"Country: {country}",
        ]

        if city_missing:
            parts.append("\nTask: BOTH city and region are missing. Please infer both.")
        else:
            parts.append(f"\nTask: City is known ({city}), but region is missing. Please infer the region only.")

        user_message = "\n".join(parts)

        _GEMINI_LIMITER.consume()
        try:
            response = self._model.generate_content(
                [system_prompt, user_message]
            )
            raw = response.text.strip()
            # Strip markdown code fences if present
            if raw.startswith("```"):
                raw = raw.split("```")[1]
                if raw.startswith("json"):
                    raw = raw[4:]
            raw = raw.strip()

            parsed = InferredLocation.model_validate_json(raw)

            # Validate region is in our known list
            if parsed.inferred_region and parsed.inferred_region not in self._region_list:
                logger.warning(
                    "[LocationResolver] Gemini returned unknown region '%s' — setting to null.",
                    parsed.inferred_region,
                )
                parsed.inferred_region = None

            # Strip common trailing suffixes from city as a safety net
            if parsed.inferred_city:
                city_val = parsed.inferred_city.strip()
                for suffix in (", Ghana", " City", " city", " Town", " town", " District", " district"):
                    if city_val.endswith(suffix):
                        city_val = city_val[: -len(suffix)].strip()
                parsed.inferred_city = city_val or None

            if city_missing:
                if not parsed.inferred_region and not parsed.inferred_city:
                    logger.warning("[LocationResolver] Gemini failed to predict BOTH region and city for '%s'", facility_name)
                else:
                    logger.info("[LocationResolver] Gemini SUCCESS (Case 2) → Predicted city='%s', region='%s' for '%s'", parsed.inferred_city, parsed.inferred_region, facility_name)
            else:
                if not parsed.inferred_region:
                    logger.warning("[LocationResolver] Gemini failed to predict region for '%s'", facility_name)
                else:
                    logger.info("[LocationResolver] Gemini SUCCESS (Case 1) → Predicted region='%s' for '%s'", parsed.inferred_region, facility_name)
            return {
                "inferred_city": parsed.inferred_city,
                "inferred_region": parsed.inferred_region,
            }

        except Exception as e:
            logger.warning(
                "[LocationResolver] Gemini call failed for '%s': %s — returning null.",
                facility_name, e,
            )
            return {"inferred_city": None, "inferred_region": None}


# ── Module-level singleton ─────────────────────────────────────────────────
try:
    _resolver = GeminiLocationResolver()
except ValueError as _err:
    logger.warning("GeminiLocationResolver disabled: %s", _err)
    _resolver = None


def resolve_location(
    facility_name: Optional[str],
    city: Optional[str],
    address_line1: Optional[str],
    address_line2: Optional[str],
    address_line3: Optional[str],
    country: str = "Ghana",
) -> dict:
    """Module-level helper — calls the singleton resolver."""
    if _resolver is None:
        return {"inferred_city": None, "inferred_region": None}
    return _resolver.resolve_location(
        facility_name=facility_name,
        city=city,
        address_line1=address_line1,
        address_line2=address_line2,
        address_line3=address_line3,
        country=country,
    )
