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


# ── Ghana city → region lookup (deterministic fallback when LLM cannot infer) ──
_GHANA_CITY_REGION: dict[str, str] = {
    # Greater Accra Region
    "accra": "Greater Accra Region",
    "tema": "Greater Accra Region",
    "dansoman": "Greater Accra Region",
    "madina": "Greater Accra Region",
    "nungua": "Greater Accra Region",
    "teshie": "Greater Accra Region",
    "lashibi": "Greater Accra Region",
    "dome": "Greater Accra Region",
    "achimota": "Greater Accra Region",
    "kasoa": "Greater Accra Region",
    # Ashanti Region
    "kumasi": "Ashanti Region",
    "obuasi": "Ashanti Region",
    "bekwai": "Ashanti Region",
    "asante mampong": "Ashanti Region",
    "ejisu": "Ashanti Region",
    "konongo": "Ashanti Region",
    "abuakwa": "Ashanti Region",  # suburb of Kumasi
    # Western Region
    "takoradi": "Western Region",
    "sekondi": "Western Region",
    "tarkwa": "Western Region",
    "prestea": "Western Region",
    "bogoso": "Western Region",
    "apremdo": "Western Region",
    "axim": "Western Region",
    "half assini": "Western Region",
    # Central Region
    "cape coast": "Central Region",
    "elmina": "Central Region",
    "winneba": "Central Region",
    "agona swedru": "Central Region",
    "mankessim": "Central Region",
    "saltpond": "Central Region",
    # Eastern Region
    "koforidua": "Eastern Region",
    "nkawkaw": "Eastern Region",
    "abomosu": "Eastern Region",
    "oda": "Eastern Region",
    "suhum": "Eastern Region",
    "nsawam": "Eastern Region",
    "akim oda": "Eastern Region",
    # Northern Region
    "tamale": "Northern Region",
    "yendi": "Northern Region",
    "walewale": "Northern Region",
    # Upper East Region
    "bolgatanga": "Upper East Region",
    "navrongo": "Upper East Region",
    "bawku": "Upper East Region",
    # Upper West Region
    "wa": "Upper West Region",
    "lawra": "Upper West Region",
    # Volta Region
    "ho": "Volta Region",
    "hohoe": "Volta Region",
    "keta": "Volta Region",
    "anloga": "Volta Region",
    # Bono Region
    "sunyani": "Bono Region",
    "berekum": "Bono Region",
    # Bono East Region
    "techiman": "Bono East Region",
    "acherensua": "Bono East Region",
    "atebubu": "Bono East Region",
    "kintampo": "Bono East Region",
    # Ahafo Region
    "goaso": "Ahafo Region",
    "kukuom": "Ahafo Region",
    # Savannah Region
    "damongo": "Savannah Region",
    "bole": "Savannah Region",
    # North East Region
    "nalerigu": "North East Region",
    "gambaga": "North East Region",
    # Oti Region
    "dambai": "Oti Region",
    # Western North Region
    "sefwi wiawso": "Western North Region",
    "bibiani": "Western North Region",
}


def _infer_ghana_region(city: Optional[str]) -> Optional[str]:
    """Lookup the Ghanaian region for a given city name (case-insensitive)."""
    if not city:
        return None
    return _GHANA_CITY_REGION.get(city.strip().lower())


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
        fac.address_stateOrRegion if fac else None,
        row.get("address_stateOrRegion"),
        _infer_ghana_region(city),  # deterministic lookup fallback
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
        fac.email[0] if fac and fac.email else None, row.get("email")
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
    
    # ── Text & Affiliations ──
    desc = _first_non_null(
        getattr(fac, "description", None) if fac else None,
        getattr(org, "organizationDescription", None) if org else None,
        row.get("description")
    )
    mission_statement = _first_non_null(
        getattr(org, "missionStatement", None) if org else None,
        row.get("missionStatement")
    )
    affiliation_types = _merge_arrays(
        getattr(fac, "affiliationTypeIds", None) if fac else None,
        _parse_csv_array(row.get("affiliationTypeIds")),
    )

    # ── Removed Confidence & Suspicious logic per user request ──

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
        "description": desc,
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
