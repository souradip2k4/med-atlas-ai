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
        "{facility} provides {item}",
        "{facility} offers the procedure: {item}",
        "The medical procedure {item} is performed at {facility}",
    ],
    "equipment": [
        "{facility} has {item}",
        "{facility} is equipped with {item}",
        "Medical equipment at {facility} includes {item}",
    ],
    "capability": [
        "{facility} supports {item}",
        "{facility} has the capability: {item}",
        "A clinical capability of {facility} is {item}",
    ],
    "specialty": [
        "{facility} specializes in {item}",
        "{facility} offers specialty care in {item}",
        "Medical specialty {item} is available at {facility}",
    ],
}


def generate_facts(facility_record: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Generate atomic facts from a single facility record.

    For each item in procedures / equipment / capabilities / specialties,
    generates 2–3 paraphrased variants using the templates.

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

    field_map = {
        "procedure": ("procedures", "procedure"),
        "equipment": ("equipment", "equipment"),
        "capability": ("capabilities", "capability"),
        "specialty": ("specialties", "specialties"),
    }

    for fact_type, (field_key, source_col) in field_map.items():
        items = facility_record.get(field_key)
        if not items:
            continue

        templates = _TEMPLATES[fact_type]

        for item in items:
            if not item or not item.strip():
                continue
            item = item.strip()

            # Generate paraphrased variants (2-3 per item)
            for tmpl in templates:
                fact_text = tmpl.format(facility=facility_name, item=item)
                facts.append({
                    "fact_id": str(uuid.uuid4()),
                    "facility_id": facility_id,
                    "fact_text": fact_text,
                    "fact_type": fact_type,
                    "source_row_id": source_row_id,
                    "source_column": source_col,
                    "source_text": item,
                })

    if not facts:
        logger.warning(
            "No facts generated for facility %s (row %s)",
            facility_name, source_row_id,
        )

    return facts
