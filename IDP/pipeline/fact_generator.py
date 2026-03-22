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
    "procedure": [
        "{facility}{location} provides {item}",
        "{facility}{location} offers the procedure: {item}",
        "The medical procedure {item} is performed at {facility}{location}",
    ],
    "equipment": [
        "{facility}{location} has {item}",
        "{facility}{location} is equipped with {item}",
        "Medical equipment at {facility}{location} includes {item}",
    ],
    "capability": [
        "{facility}{location} supports {item}",
        "{facility}{location} has the capability: {item}",
        "A clinical capability of {facility}{location} is {item}",
    ],
    "specialty": [
        "{facility}{location} specializes in {item}",
        "{facility}{location} offers specialty care in {item}",
        "Medical specialty {item} is available at {facility}{location}",
    ],
}


def _make_fact(facility_id: str, source_row_id: str, fact_type: str,
               fact_text: str, source_column: str, source_text: str) -> Dict[str, Any]:
    """Helper to build a single fact dict."""
    return {
        "fact_id": str(uuid.uuid4()),
        "facility_id": facility_id,
        "fact_text": fact_text,
        "fact_type": fact_type,
        "source_row_id": source_row_id,
        "source_column": source_column,
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
    source_row_id = facility_record.get("source_row_id", "")

    # ── Array fields (paraphrased) ───────────────────────────────────────
    field_map = {
        "procedure": ("procedures", "procedure"),
        "equipment": ("equipment", "equipment"),
        "capability": ("capabilities", "capability"),
        "specialty": ("specialties", "specialties"),
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

    for fact_type, (field_key, source_col) in field_map.items():
        items = facility_record.get(field_key)
        if not items:
            continue

        templates = _TEMPLATES[fact_type]

        for item in items:
            if not item or not item.strip():
                continue
            item = item.strip()

            # Generate paraphrased variants (2-3 per item) — securely handling missing location
            for tmpl in templates:
                fact_text = tmpl.format(facility=facility_name, item=item, location=loc_str)
                facts.append(_make_fact(
                    facility_id, source_row_id, fact_type,
                    fact_text, source_col, item,
                ))

    # ── Scalar fields (single sentence each) ────────────────────────────

    # Capacity
    capacity = facility_record.get("capacity")
    if capacity:
        facts.append(_make_fact(
            facility_id, source_row_id, "capacity",
            f"{facility_name}{loc_str} has an inpatient capacity of {capacity} beds.",
            "capacity", str(capacity),
        ))

    # Number of doctors
    number_doctors = facility_record.get("number_doctors")
    if number_doctors:
        facts.append(_make_fact(
            facility_id, source_row_id, "workforce",
            f"{facility_name}{loc_str} has {number_doctors} medical doctors on staff.",
            "number_doctors", str(number_doctors),
        ))

    # Organization type
    org_type = facility_record.get("organization_type")
    if org_type:
        facts.append(_make_fact(
            facility_id, source_row_id, "organization_type",
            f"{facility_name}{loc_str} is a {org_type}.",
            "organization_type", org_type,
        ))

    # Accepts volunteers
    accepts_volunteers = facility_record.get("accepts_volunteers")
    if accepts_volunteers is True:
        facts.append(_make_fact(
            facility_id, source_row_id, "volunteers",
            f"{facility_name}{loc_str} accepts clinical volunteers.",
            "accepts_volunteers", "true",
        ))
    elif accepts_volunteers is False:
        facts.append(_make_fact(
            facility_id, source_row_id, "volunteers",
            f"{facility_name}{loc_str} does not currently accept clinical volunteers.",
            "accepts_volunteers", "false",
        ))

    # Year established
    year_established = facility_record.get("year_established")
    if year_established:
        facts.append(_make_fact(
            facility_id, source_row_id, "history",
            f"{facility_name}{loc_str} was established in {year_established}.",
            "year_established", str(year_established),
        ))

    # Location
    address_line1 = facility_record.get("address_line1")
    address_line2 = facility_record.get("address_line2")
    address_line3 = facility_record.get("address_line3")

    addr_parts = []
    if address_line1: addr_parts.append(address_line1.strip())
    if address_line2: addr_parts.append(address_line2.strip())
    if address_line3: addr_parts.append(address_line3.strip())

    full_street_address = ", ".join(addr_parts)

    if loc_str:
        if full_street_address:
            location_text = f"{facility_name} is located at {full_street_address}{loc_str}."
        else:
            location_text = f"{facility_name} is located{loc_str}."
            
        provenance_str = f"{full_street_address or ''}{loc_str}".strip(", ")
        facts.append(_make_fact(
            facility_id, source_row_id, "location",
            location_text, "address", provenance_str,
        ))

    # Description
    desc = facility_record.get("description")
    if desc:
        facts.append(_make_fact(
            facility_id, source_row_id, "description",
            f"Description of {facility_name}{loc_str}: {desc}",
            "description", desc,
        ))

    # Mission Statement
    mission = facility_record.get("mission_statement")
    if mission:
        facts.append(_make_fact(
            facility_id, source_row_id, "mission_statement",
            f"The mission statement of {facility_name}{loc_str} is: {mission}",
            "mission_statement", mission,
        ))

    # Affiliation Types
    affiliations = facility_record.get("affiliation_types")
    if affiliations:
        affiliations_str = ", ".join(affiliations)
        facts.append(_make_fact(
            facility_id, source_row_id, "affiliation",
            f"{facility_name}{loc_str} is affiliated with the following types: {affiliations_str}.",
            "affiliation_types", affiliations_str,
        ))

    # Operator Type
    operator_type = facility_record.get("operator_type")
    if operator_type:
        op_text = "privately operated" if operator_type == "private" else "publicly operated"
        facts.append(_make_fact(
            facility_id, source_row_id, "organization_type",
            f"{facility_name}{loc_str} is {op_text}.",
            "operator_type", operator_type,
        ))
        
    # Facility Type
    facility_type = facility_record.get("facility_type")
    if facility_type:
        facts.append(_make_fact(
            facility_id, source_row_id, "organization_type",
            f"{facility_name}{loc_str} is classified as a {facility_type}.",
            "facility_type", facility_type,
        ))

    if not facts:
        logger.warning(
            "No facts generated for facility %s (row %s)",
            facility_name, source_row_id,
        )

    return facts
