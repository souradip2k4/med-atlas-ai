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
        "{facility} in {city}, {country} provides {item}",
        "{facility} in {city}, {country} offers the procedure: {item}",
        "The medical procedure {item} is performed at {facility} in {city}, {country}",
    ],
    "equipment": [
        "{facility} in {city}, {country} has {item}",
        "{facility} in {city}, {country} is equipped with {item}",
        "Medical equipment at {facility} in {city}, {country} includes {item}",
    ],
    "capability": [
        "{facility} in {city}, {country} supports {item}",
        "{facility} in {city}, {country} has the capability: {item}",
        "A clinical capability of {facility} in {city}, {country} is {item}",
    ],
    "specialty": [
        "{facility} in {city}, {country} specializes in {item}",
        "{facility} in {city}, {country} offers specialty care in {item}",
        "Medical specialty {item} is available at {facility} in {city}, {country}",
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

    city = facility_record.get("city") or "Unknown City"
    country = facility_record.get("country") or "Unknown Country"

    for fact_type, (field_key, source_col) in field_map.items():
        items = facility_record.get(field_key)
        if not items:
            continue

        templates = _TEMPLATES[fact_type]

        for item in items:
            if not item or not item.strip():
                continue
            item = item.strip()

            # Generate paraphrased variants (2-3 per item) — all location-enriched
            for tmpl in templates:
                fact_text = tmpl.format(facility=facility_name, item=item, city=city, country=country)
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
            f"{facility_name} has an inpatient capacity of {capacity} beds.",
            "capacity", str(capacity),
        ))

    # Number of doctors
    number_doctors = facility_record.get("number_doctors")
    if number_doctors:
        facts.append(_make_fact(
            facility_id, source_row_id, "workforce",
            f"{facility_name} has {number_doctors} medical doctors on staff.",
            "number_doctors", str(number_doctors),
        ))

    # Organization type
    org_type = facility_record.get("organization_type")
    if org_type:
        facts.append(_make_fact(
            facility_id, source_row_id, "organization_type",
            f"{facility_name} is a {org_type}.",
            "organization_type", org_type,
        ))

    # Accepts volunteers
    accepts_volunteers = facility_record.get("accepts_volunteers")
    if accepts_volunteers is True:
        facts.append(_make_fact(
            facility_id, source_row_id, "volunteers",
            f"{facility_name} accepts clinical volunteers.",
            "accepts_volunteers", "true",
        ))
    elif accepts_volunteers is False:
        facts.append(_make_fact(
            facility_id, source_row_id, "volunteers",
            f"{facility_name} does not currently accept clinical volunteers.",
            "accepts_volunteers", "false",
        ))

    # Year established
    year_established = facility_record.get("year_established")
    if year_established:
        facts.append(_make_fact(
            facility_id, source_row_id, "history",
            f"{facility_name} was established in {year_established}.",
            "year_established", str(year_established),
        ))

    # Location
    city = facility_record.get("city")
    country = facility_record.get("country")
    address_line1 = facility_record.get("address_line1")
    if city and country:
        if address_line1:
            location_text = f"{facility_name} is located at {address_line1}, {city}, {country}."
        else:
            location_text = f"{facility_name} is located in {city}, {country}."
        facts.append(_make_fact(
            facility_id, source_row_id, "location",
            location_text, "address", f"{address_line1 or ''}, {city}, {country}".strip(", "),
        ))

    # Description
    desc = facility_record.get("description")
    if desc:
        facts.append(_make_fact(
            facility_id, source_row_id, "description",
            f"Description of {facility_name}: {desc}",
            "description", desc,
        ))

    if not facts:
        logger.warning(
            "No facts generated for facility %s (row %s)",
            facility_name, source_row_id,
        )

    return facts
