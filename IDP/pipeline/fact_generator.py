"""
fact_generator.py — Generate atomic facts from structured facility records.

Each fact gets:
  - fact_id (UUID)
  - fact_type (procedure / equipment / capability / specialty)
  - provenance: source_row_id, source_column, source_text
  - 2-3 paraphrased variants per fact for better embedding recall
"""

import uuid
import logging
from typing import Any, Dict, List

logger = logging.getLogger(__name__)

# ── Fact templates ───────────────────────────────────────────────────────

_TEMPLATES = {
    "procedure": "{facility}{location} provides the following medical procedures: {items}.",
    "equipment": "{facility}{location} is equipped with: {items}.",
    "capability": "{facility}{location} has the following clinical capabilities: {items}.",
    "specialty": "{facility}{location} offers specialty care in: {items}.",
}


def _make_fact(facility_id: str, fact_type: str,
               fact_text: str, source_text: str) -> Dict[str, Any]:
    """Helper to build a single fact dict."""
    return {
        "fact_id": str(uuid.uuid4()),
        "facility_id": facility_id,
        "fact_text": fact_text,
        "fact_type": fact_type,
        "source_text": source_text,
    }


def generate_facts(facility_record: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Generate atomic facts from a single facility record.

    For each item in procedures / equipment / capabilities / specialties,
    generates 2–3 paraphrased variants using the templates.
    Also generates single-sentence facts for all scalar fields.

    Parameters
    ----------
    facility_record : dict
        A single row from the ``facility_records`` table.

    Returns
    -------
    list[dict]
        List of fact dicts matching ``FACILITY_FACTS_SCHEMA``.
    """
    facts: List[Dict[str, Any]] = []
    facility_name = facility_record.get("facility_name", "Unknown")
    facility_id = facility_record.get("facility_id", "")

    # ── Array fields (paraphrased) ───────────────────────────────────────
    field_map = {
        "procedure": "procedures",
        "equipment": "equipment",
        "capability": "capabilities",
        "specialty": "specialties",
    }

    city = facility_record.get("city")
    state = facility_record.get("state")
    country = facility_record.get("country")
    
    parts = []
    if city:
        parts.append(city.strip())
    if state:
        parts.append(state.strip())
    if country:
        parts.append(country.strip())
        
    loc_str = f" in {', '.join(parts)}" if parts else ""

    for fact_type, field_key in field_map.items():
        items = facility_record.get(field_key)
        if not items:
            continue

        clean_items = [item.strip() for item in items if item and item.strip()]
        if not clean_items:
            continue

        template = _TEMPLATES[fact_type]
        items_str = ", ".join(clean_items)
        fact_text = template.format(facility=facility_name, items=items_str, location=loc_str)
        
        facts.append(_make_fact(
            facility_id, fact_type,
            fact_text, items_str,
        ))

    # ── Summary Fact (identity-focused: name + type + location + affiliation only) ──
    summary_parts = []

    # 1. Core identity sentence
    operator_type = facility_record.get("operator_type")
    facility_type = facility_record.get("facility_type")
    op_str = "privately operated " if operator_type == "private" else "publicly operated " if operator_type else ""
    fac_str = facility_type if facility_type else "organization"

    # e.g. "Bromley Park Dental Clinic is a privately operated clinic in Accra, Greater Accra Region, Ghana."
    summary_parts.append(f"{facility_name} is a {op_str}{fac_str}{loc_str}.")

    # 2. Affiliation sentence
    affiliations = facility_record.get("affiliation_types")
    if affiliations:
        summary_parts.append(f"It is affiliated with the following types: {', '.join(affiliations)}.")

    if summary_parts:
        summary_text = " ".join(summary_parts)
        facts.append(_make_fact(
            facility_id, "summary",
            summary_text, summary_text
        ))

    # ── Description Fact (new) ────────────────────────────────────────────────
    # Combines description + mission_statement with facility_name + location prefix.
    # No fact row is created at all if both fields are null/empty.
    desc_val = facility_record.get("description")
    mission_val = facility_record.get("mission_statement")

    if desc_val and mission_val:
        desc_body = f"{desc_val} Its mission is: {mission_val}"
    elif desc_val:
        desc_body = desc_val
    elif mission_val:
        desc_body = f"Its mission is: {mission_val}"
    else:
        desc_body = None

    if desc_body:
        # Prefix with facility_name + location, consistent with all other fact_types
        desc_text = f"{facility_name}{loc_str}: {desc_body}"
        facts.append(_make_fact(
            facility_id, "description",
            desc_text, desc_body
        ))

    if not facts:
        logger.warning(
            "No facts generated for facility %s (id: %s)",
            facility_name, facility_id,
        )

    return facts