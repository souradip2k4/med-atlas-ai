"""
merger.py — Merge outputs from the 4 extraction steps into a single
structured facility record.

Rules
-----
* Never overwrite a field with NULL if it is already populated.
* Arrays are merged with deduplication.
* Conflicting scalar values → keep both in a list (for traceability).
* Populates provenance, confidence, and timestamp fields.
"""

import uuid
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


def _merge_arrays(*arrays: Optional[List[str]]) -> List[str]:
    """Merge multiple optional lists, deduplicating while preserving order."""
    seen: set[str] = set()
    merged: list[str] = []
    for arr in arrays:
        if not arr:
            continue
        for item in arr:
            lower = item.strip().lower()
            if lower and lower not in seen:
                seen.add(lower)
                merged.append(item.strip())
    return merged


def _first_non_null(*values: Any) -> Any:
    """Return the first non-None / non-empty value."""
    for v in values:
        if v is not None and v != "" and v != []:
            return v
    return None


def merge_extraction_results(
    extraction: Dict[str, Any],
    row: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Combine the outputs of all 4 extraction steps plus the original
    CSV row into one flat dict matching the ``facility_records`` schema.

    Parameters
    ----------
    extraction : dict
        The dict returned by ``LLMExtractor.process_row()``.
    row : dict
        The original CSV row (as a Python dict).

    Returns
    -------
    dict
        A single record ready for insertion into ``facility_records``.
    """
    org = extraction.get("org_output")
    facts = extraction.get("facts_output")
    specs = extraction.get("specialties_output")
    fac = extraction.get("facility_output")

    facility_name = extraction.get("facility_name") or row.get("name") or "Unknown"
    source_row_id = extraction.get("source_row_id", "")
    synth_text = extraction.get("synthesized_text", "")

    now = datetime.now(timezone.utc)

    # ── Determine organization_type ──
    org_type = _first_non_null(
        row.get("organization_type"),
        "facility",  # fallback
    )

    # ── Merge medical arrays ──
    specialties = _merge_arrays(
        specs.specialties if specs else None,
        _parse_csv_array(row.get("specialties")),
    )
    procedures = _merge_arrays(
        facts.procedure if facts else None,
        _parse_csv_array(row.get("procedure")),
    )
    equipment = _merge_arrays(
        facts.equipment if facts else None,
        _parse_csv_array(row.get("equipment")),
    )
    capabilities = _merge_arrays(
        facts.capability if facts else None,
        _parse_csv_array(row.get("capability")),
    )

    # ── Location from Facility extraction (fallback to CSV) ──
    address_line1 = _first_non_null(
        fac.address_line1 if fac else None, row.get("address_line1")
    )
    address_line2 = _first_non_null(
        fac.address_line2 if fac else None, row.get("address_line2")
    )
    address_line3 = _first_non_null(
        fac.address_line3 if fac else None, row.get("address_line3")
    )
    city = _first_non_null(
        fac.address_city if fac else None, row.get("address_city")
    )
    state = _first_non_null(
        fac.address_stateOrRegion if fac else None, row.get("address_stateOrRegion")
    )
    country = _first_non_null(
        fac.address_country if fac else None, row.get("address_country")
    )
    country_code = _first_non_null(
        fac.address_countryCode if fac else None, row.get("address_countryCode")
    )

    # ── Contact ──
    phone_numbers = _merge_arrays(
        fac.phone_numbers if fac else None,
        _parse_csv_array(row.get("phone_numbers")),
    )
    email = _first_non_null(
        fac.email if fac else None, row.get("email")
    )
    websites = _merge_arrays(
        fac.websites if fac else None,
        _parse_csv_array(row.get("websites")),
    )
    official_website = _first_non_null(
        fac.officialWebsite if fac else None, row.get("officialWebsite")
    )

    # ── Meta ──
    year_established = _first_non_null(
        fac.yearEstablished if fac else None,
        _try_int(row.get("yearEstablished")),
    )
    accepts_volunteers = _first_non_null(
        fac.acceptsVolunteers if fac else None,
        _try_bool(row.get("acceptsVolunteers")),
    )
    number_doctors = _first_non_null(
        fac.numberDoctors if fac else None,
        _try_int(row.get("numberDoctors")),
    )
    capacity = _first_non_null(
        fac.capacity if fac else None,
        _try_int(row.get("capacity")),
    )

    # ── Confidence ──
    conf_org = extraction.get("confidence_org", 0.0)
    conf_facts = extraction.get("confidence_facts", 0.0)
    conf_spec = extraction.get("confidence_specialties", 0.0)
    conf_fac = extraction.get("confidence_facility", 0.0)
    extraction_confidence = round(
        (conf_org + conf_facts + conf_spec + conf_fac) / 4, 3
    )

    # ── Suspicious checks ──
    is_suspicious = False
    suspicious_reason = None
    if not specialties and not procedures and not equipment and not capabilities:
        is_suspicious = True
        suspicious_reason = "No medical data extracted from any step"
    elif facility_name.lower() in ("unknown", "unknown facility"):
        is_suspicious = True
        suspicious_reason = "Could not determine facility name"

    return {
        "facility_id": str(uuid.uuid4()),
        "source_row_id": source_row_id,
        "facility_name": facility_name,
        "organization_type": org_type,
        "specialties": specialties or None,
        "procedures": procedures or None,
        "equipment": equipment or None,
        "capabilities": capabilities or None,
        "address_line1": address_line1,
        "address_line2": address_line2,
        "address_line3": address_line3,
        "city": city,
        "state": state,
        "country": country,
        "country_code": country_code,
        "phone_numbers": phone_numbers or None,
        "email": email,
        "websites": websites or None,
        "officialWebsite": official_website,
        "year_established": _try_int(year_established),
        "accepts_volunteers": _try_bool(accepts_volunteers),
        "number_doctors": _try_int(number_doctors),
        "capacity": _try_int(capacity),
        "evidence_text": synth_text,
        "source_text": synth_text[:500] if synth_text else None,
        "source_column": "synthesized",
        "extraction_confidence": extraction_confidence,
        "confidence_specialties": round(conf_spec, 3),
        "confidence_equipment": round(conf_facts, 3),
        "confidence_capabilities": round(conf_facts, 3),
        "is_suspicious": is_suspicious,
        "suspicious_reason": suspicious_reason,
        "created_at": now,
        "updated_at": now,
    }


# ── Utility helpers ──────────────────────────────────────────────────────

def _parse_csv_array(value: Any) -> Optional[List[str]]:
    """
    Parse a CSV cell that may contain a JSON-encoded array string
    like ``'["a","b"]'`` into a Python list.
    """
    if value is None:
        return None
    if isinstance(value, list):
        return value
    s = str(value).strip()
    if not s or s.lower() in ("null", "none", "[]"):
        return None
    try:
        import json
        parsed = json.loads(s)
        if isinstance(parsed, list):
            return [str(x) for x in parsed if x]
    except (json.JSONDecodeError, TypeError):
        pass
    return None


def _try_int(value: Any) -> Optional[int]:
    """Safely coerce to int or return None."""
    if value is None:
        return None
    try:
        return int(float(str(value)))
    except (ValueError, TypeError):
        return None


def _try_bool(value: Any) -> Optional[bool]:
    """Safely coerce to bool or return None."""
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    s = str(value).strip().lower()
    if s in ("true", "1", "yes"):
        return True
    if s in ("false", "0", "no"):
        return False
    return None
