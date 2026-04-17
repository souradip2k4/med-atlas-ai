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

import re
import uuid
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pipeline.geocoder import FacilityGeocoder

logger = logging.getLogger(__name__)

# Module-level singleton — one geolocator shared across all rows
# (avoids re-creating the HTTP session for each row)
try:
    _geocoder = FacilityGeocoder()
except ValueError as _geo_err:
    logger.warning("Geocoder disabled: %s", _geo_err)
    _geocoder = None


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


def _extract_bed_count(arrays: list[Optional[List[str]]]) -> Optional[int]:
    """Scan capability/equipment strings for bed/capacity numbers.

    Used as a fallback when the LLM did not extract ``capacity`` as a
    structured integer — the data may still be present as free-text
    like "Maintains 15 wards with 300 operational beds".
    """
    patterns = [
        r'(\d+)\s*[-–]?\s*beds?\b',                           # "300 beds", "39-bed"
        r'bed\s*capacity\s*(?:of\s*)?(\d+)',                   # "bed capacity of 39"
        r'(\d+)\s*operational\s*beds',                         # "300 operational beds"
        r'capacity\s*(?:of|to)\s*(?:accommodate\s*)?(\d+)',    # "capacity to accommodate 600"
    ]
    for arr in arrays:
        if not arr:
            continue
        for item in arr:
            for pat in patterns:
                m = re.search(pat, item, re.IGNORECASE)
                if m:
                    return int(m.group(1))
    return None


def _extract_doctor_count(arrays: list[Optional[List[str]]]) -> Optional[int]:
    """Scan capability/procedure strings for doctor/staff count numbers.

    Used as a fallback when the CSV ``numberDoctors`` field is null.
    Looks for patterns like "10 doctors", "staff of 5 physicians", "3 medical officers".
    """
    patterns = [
        r'(\d+)\s*[-–]?\s*doctors?\b',                          # "10 doctors"
        r'(\d+)\s*[-–]?\s*physicians?\b',                       # "5 physicians"
        r'(\d+)\s*[-–]?\s*medical\s*officers?',                 # "3 medical officers"
        r'staff\s*(?:of|:)?\s*(\d+)',                            # "staff of 12"
        r'(\d+)\s*clinical\s*staff',                             # "8 clinical staff"
    ]
    for arr in arrays:
        if not arr:
            continue
        for item in arr:
            for pat in patterns:
                m = re.search(pat, item, re.IGNORECASE)
                if m:
                    return int(m.group(1))
    return None




# ── Ghana city → region lookup (deterministic fallback when LLM cannot infer) ──
_GHANA_CITY_REGION: dict[str, str] = {
    # ── Greater Accra Region ──────────────────────────────────────────────
    "accra": "Greater Accra Region",
    "tema": "Greater Accra Region",
    "dansoman": "Greater Accra Region",
    "central dansoman": "Greater Accra Region",
    "madina": "Greater Accra Region",
    "nungua": "Greater Accra Region",
    "teshie": "Greater Accra Region",
    "lashibi": "Greater Accra Region",
    "dome": "Greater Accra Region",
    "achimota": "Greater Accra Region",
    "kasoa": "Greater Accra Region",
    "accra newtown": "Greater Accra Region",
    "adenta": "Greater Accra Region",
    "adenta housing": "Greater Accra Region",
    "airport residential": "Greater Accra Region",
    "labone": "Greater Accra Region",
    "cantonments": "Greater Accra Region",
    "east legon": "Greater Accra Region",
    "north legon": "Greater Accra Region",
    "legon": "Greater Accra Region",
    "haatso": "Greater Accra Region",
    "spintex": "Greater Accra Region",
    "north kaneshie": "Greater Accra Region",
    "kaneshie": "Greater Accra Region",
    "dzorwulu": "Greater Accra Region",
    "asylum down": "Greater Accra Region",
    "osu": "Greater Accra Region",
    "labadi": "Greater Accra Region",
    "la": "Greater Accra Region",
    "kanda": "Greater Accra Region",
    "okponglo": "Greater Accra Region",
    "korle bu": "Greater Accra Region",
    "jamestown": "Greater Accra Region",
    "agbogbloshie": "Greater Accra Region",
    "abossey okai": "Greater Accra Region",
    "kokomlemle": "Greater Accra Region",
    "ring road": "Greater Accra Region",
    "aviation": "Greater Accra Region",
    "community 25": "Greater Accra Region",
    "community 1": "Greater Accra Region",
    "community 22": "Greater Accra Region",
    "ada foah": "Greater Accra Region",
    "prampram": "Greater Accra Region",
    "weija": "Greater Accra Region",
    "amasaman": "Greater Accra Region",
    "pokuase": "Greater Accra Region",
    "ashiaman": "Greater Accra Region",
    "odokor": "Greater Accra Region",
    "darkuman": "Greater Accra Region",
    "bubiashie": "Greater Accra Region",
    # ── Ashanti Region ────────────────────────────────────────────────────
    "kumasi": "Ashanti Region",
    "obuasi": "Ashanti Region",
    "bekwai": "Ashanti Region",
    "asante mampong": "Ashanti Region",
    "ejisu": "Ashanti Region",
    "konongo": "Ashanti Region",
    "abuakwa": "Ashanti Region",
    "atonsu": "Ashanti Region",
    "atonsu kumasi": "Ashanti Region",
    "ahodwo": "Ashanti Region",
    "asokore": "Ashanti Region",
    "asokore mampong": "Ashanti Region",
    "asokwa": "Ashanti Region",
    "suame": "Ashanti Region",
    "nhyiaeso": "Ashanti Region",
    "bantama": "Ashanti Region",
    "dichemso": "Ashanti Region",
    "kwadaso": "Ashanti Region",
    "tafo": "Ashanti Region",
    "tanoso": "Ashanti Region",
    "manhyia": "Ashanti Region",
    "danyame": "Ashanti Region",
    "ayigya": "Ashanti Region",
    "ampa": "Ashanti Region",
    "ksi": "Ashanti Region",
    "mampong": "Ashanti Region",
    "agogo": "Ashanti Region",
    "juaben": "Ashanti Region",
    "ashanti new town": "Ashanti Region",
    "bosomtwe": "Ashanti Region",
    "nkawie": "Ashanti Region",
    "tepa": "Ashanti Region",
    "manso nkwanta": "Ashanti Region",
    "ofinso": "Ashanti Region",
    "drobonso": "Ashanti Region",
    # ── Western Region ────────────────────────────────────────────────────
    "takoradi": "Western Region",
    "sekondi": "Western Region",
    "tarkwa": "Western Region",
    "prestea": "Western Region",
    "bogoso": "Western Region",
    "apremdo": "Western Region",
    "axim": "Western Region",
    "half assini": "Western Region",
    "shama": "Western Region",
    "effia": "Western Region",
    "nsuta": "Western Region",
    "aboso": "Western Region",
    "agona": "Western Region",
    "nkroful": "Western Region",
    "ellembele": "Western Region",
    "abura": "Western Region",
    # ── Central Region ────────────────────────────────────────────────────
    "cape coast": "Central Region",
    "elmina": "Central Region",
    "winneba": "Central Region",
    "agona swedru": "Central Region",
    "mankessim": "Central Region",
    "saltpond": "Central Region",
    "assin fosu": "Central Region",
    "breman asikuma": "Central Region",
    "mfantsiman": "Central Region",
    "eguafo abrem": "Central Region",
    "ajumako": "Central Region",
    "gomoa": "Central Region",
    "twifo praso": "Central Region",
    # ── Eastern Region ────────────────────────────────────────────────────
    "koforidua": "Eastern Region",
    "nkawkaw": "Eastern Region",
    "abomosu": "Eastern Region",
    "oda": "Eastern Region",
    "suhum": "Eastern Region",
    "nsawam": "Eastern Region",
    "akim oda": "Eastern Region",
    "akosombo": "Eastern Region",
    "atimpoku": "Eastern Region",
    "somanya": "Eastern Region",
    "abetifi": "Eastern Region",
    "mpraeso": "Eastern Region",
    "nkurakan": "Eastern Region",
    "anum": "Eastern Region",
    "asamankese": "Eastern Region",
    "mangoase": "Eastern Region",
    "osino": "Eastern Region",
    "kukurantumi": "Eastern Region",
    "tafo koforidua": "Eastern Region",
    # ── Volta Region ──────────────────────────────────────────────────────
    "ho": "Volta Region",
    "hohoe": "Volta Region",
    "keta": "Volta Region",
    "anloga": "Volta Region",
    "akatsi": "Volta Region",
    "adidome": "Volta Region",
    "sogakofe": "Volta Region",
    "battor": "Volta Region",
    "anfoega": "Volta Region",
    "kpando": "Volta Region",
    "aflao": "Volta Region",
    "denu": "Volta Region",
    "abor": "Volta Region",
    "tsito": "Volta Region",
    "vane": "Volta Region",
    "peki": "Volta Region",
    "jasikan": "Volta Region",
    "kpeve": "Volta Region",
    "nkwanta": "Volta Region",
    # ── Northern Region ───────────────────────────────────────────────────
    "tamale": "Northern Region",
    "yendi": "Northern Region",
    "walewale": "Northern Region",
    "savelugu": "Northern Region",
    "gushegu": "Northern Region",
    "karaga": "Northern Region",
    "tolon": "Northern Region",
    "kumbungu": "Northern Region",
    "bimbilla": "Northern Region",
    # ── Upper East Region ─────────────────────────────────────────────────
    "bolgatanga": "Upper East Region",
    "navrongo": "Upper East Region",
    "bawku": "Upper East Region",
    "zebilla": "Upper East Region",
    "sandema": "Upper East Region",
    "paga": "Upper East Region",
    "chiana": "Upper East Region",
    # ── Upper West Region ─────────────────────────────────────────────────
    "wa": "Upper West Region",
    "lawra": "Upper West Region",
    "jirapa": "Upper West Region",
    "nandom": "Upper West Region",
    "tumu": "Upper West Region",
    "kaleo": "Upper West Region",
    # ── Bono Region ───────────────────────────────────────────────────────
    "sunyani": "Bono Region",
    "berekum": "Bono Region",
    "dormaa ahenkro": "Bono Region",
    "wenchi": "Bono Region",
    "drobo": "Bono Region",
    "sampa": "Bono Region",
    # ── Bono East Region ──────────────────────────────────────────────────
    "techiman": "Bono East Region",
    "acherensua": "Bono East Region",
    "atebubu": "Bono East Region",
    "kintampo": "Bono East Region",
    "nkoranza": "Bono East Region",
    "yeji": "Bono East Region",
    # ── Ahafo Region ──────────────────────────────────────────────────────
    "goaso": "Ahafo Region",
    "kukuom": "Ahafo Region",
    "hwidiem": "Ahafo Region",
    "kenyasi": "Ahafo Region",
    # ── Savannah Region ───────────────────────────────────────────────────
    "damongo": "Savannah Region",
    "bole": "Savannah Region",
    "sawla": "Savannah Region",
    "buipe": "Savannah Region",
    # ── North East Region ─────────────────────────────────────────────────
    "nalerigu": "North East Region",
    "gambaga": "North East Region",
    "chereponi": "North East Region",
    # ── Oti Region ────────────────────────────────────────────────────────
    "dambai": "Oti Region",
    "krachi": "Oti Region",
    "nkwanta south": "Oti Region",
    # ── Western North Region ──────────────────────────────────────────────
    "sefwi wiawso": "Western North Region",
    "bibiani": "Western North Region",
    "enchi": "Western North Region",
    "juaboso": "Western North Region",
    "sefwi akontombra": "Western North Region",
}


def _infer_ghana_region(city: Optional[str]) -> Optional[str]:
    """Lookup the Ghanaian region for a given city name (case-insensitive).

    Resolution order (stops at first match):
    1. Exact match on the full city string.
    2. Partial match — checks whether any known key appears as a
       substring of the city string, ordered by key length descending
       so longer (more specific) keys win over shorter ones.
       e.g. "Atonsu Kumasi" → "Ashanti Region" via key "kumasi".
    """
    if not city:
        return None
    city_lower = city.strip().lower()

    # 1. Exact match (fast, no change to existing behaviour)
    if city_lower in _GHANA_CITY_REGION:
        return _GHANA_CITY_REGION[city_lower]

    # 2. Partial/substring match — longer keys checked first to avoid
    #    short keys (e.g. "wa", "ho", "la") matching unintended strings.
    for key in sorted(_GHANA_CITY_REGION, key=len, reverse=True):
        if len(key) >= 4 and key in city_lower:   # min 4 chars to avoid false positives
            return _GHANA_CITY_REGION[key]

    return None


def _clean_array(arr):
    """Delete junk location/contact/directory strings from medical arrays."""
    if not arr:
        return []
    junk_killers = [
        "located in", "located at", "located on", "location:",
        "phone:", "email:", "website:", "contact number", "opening hours", "always open",
        "facebook", "followers", "likes,", "listed in", "registered with",
        "ghanayello", "ghanabusinessweb", "page created", "unofficial page", "phone number",
    ]
    return [
        item for item in arr
        if not any(junk in item.lower() for junk in junk_killers)
    ]


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
    org = extraction.get("org_output")  # Always None now (Step 1 removed)
    facts = extraction.get("facts_output")
    specs = extraction.get("specialties_output")  # Always None (CSV used directly)
    fac = extraction.get("facility_output")        # Always None (CSV used directly)

    # facility_name: prefer LLM-cleaned name, then row primary (shortest variant from deduplicator)
    facility_name = (
        (facts.cleaned_name.strip() if facts and facts.cleaned_name else None)
        or extraction.get("facility_name")
        or row.get("name")
        or "Unknown"
    )
    source_row_id = extraction.get("source_row_id", "")
    now = datetime.now(timezone.utc)

    # ── Determine organization_type — directly from CSV (Step 1 removed) ──
    org_type = row.get("organization_type") or None

    # ── Medical arrays: use LLM-validated output directly (no re-merge with CSV) ──
    # The LLM has already cleaned the pre-merged arrays. _clean_array acts as safety net.
    specialties = _clean_array(
        facts.specialties if facts else None
    ) or _parse_csv_array(row.get("specialties")) or None
    procedures = _clean_array(
        facts.procedure if facts else None
    ) or _parse_csv_array(row.get("procedure")) or None
    equipment = _clean_array(
        facts.equipment if facts else None
    ) or _parse_csv_array(row.get("equipment")) or None
    capabilities = _clean_array(
        facts.capability if facts else None
    ) or _parse_csv_array(row.get("capability")) or None

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
        row.get("address_stateorregion"),
        _infer_ghana_region(city),  # deterministic lookup fallback
    )
    country = _first_non_null(
        fac.address_country if fac else None, row.get("address_country")
    )
    country_code = _first_non_null(
        fac.address_countryCode if fac else None, row.get("address_countrycode")
    )

    # ── Geocoding (lat/lon) ──
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    if _geocoder is not None:
        geo = _geocoder.geocode_facility(
            name=facility_name,
            city=city,
            state=state,
            country=country or "Ghana",
        )
        latitude = geo["latitude"]
        longitude = geo["longitude"]
        # Backfill city/state when the API resolved them and we had no value
        if geo["resolved_city"] and not city:
            city = geo["resolved_city"]
        if geo["resolved_state"] and not state:
            state = geo["resolved_state"]

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
        fac.officialWebsite if fac else None, row.get("officialwebsite")
    )

    # ── Meta ──
    year_established = _first_non_null(
        fac.yearEstablished if fac else None,
        _try_int(row.get("yearestablished")),
    )
    accepts_volunteers = _first_non_null(
        fac.acceptsVolunteers if fac else None,
        _try_bool(row.get("acceptsvolunteers")),
    )
    capacity = _first_non_null(
        _try_int(row.get("capacity")),                                 # CSV column (primary)
        getattr(facts, "capacity", None) if facts else None,           # Step 2 LLM (secondary)
        fac.capacity if fac else None,                                 # Step 4 legacy (now None)
    )

    # ── Regex fallback: recover bed counts from free-text ──
    if capacity is None:
        capacity = _extract_bed_count([capabilities, equipment])

    # ── Doctor count: CSV primary → LLM secondary → free-text fallback ──
    no_doctors = _first_non_null(
        _try_int(row.get("numberdoctors")),                            # CSV column (primary)
        getattr(facts, "noDocors", None) if facts else None,           # Step 2 LLM (secondary)
    )
    if no_doctors is None:
        no_doctors = _extract_doctor_count([procedures, capabilities])

    # ── Text & Affiliations ──
    desc = _first_non_null(
        row.get("description"),                                  # CSV original (primary — preserve as-is)
        getattr(facts, "description", None) if facts else None,  # Step 2 LLM-generated (fallback for null CSV rows)
        getattr(org, "organizationDescription", None) if org else None,
        getattr(fac, "description", None) if fac else None,      # Step 4 (legacy, now None)
    )
    mission_statement = _first_non_null(
        row.get("missionstatement"),                             # CSV original
        getattr(org, "missionStatement", None) if org else None,
    )
    affiliation_types = _merge_arrays(
        getattr(fac, "affiliationTypeIds", None) if fac else None,
        _parse_csv_array(row.get("affiliationtypeids")),
    )
    operator_type = _first_non_null(
        getattr(fac, "operatorTypeId", None) if fac else None,
        row.get("operatortypeid")
    )
    facility_type = _first_non_null(
        getattr(fac, "facilityTypeId", None) if fac else None,
        row.get("facilitytypeid"),
        row.get("classification")
    )

    social_dict = {}
    if facebook_link := row.get("facebooklink"): social_dict["facebookLink"] = facebook_link
    if twitter_link := row.get("twitterlink"): social_dict["twitterLink"] = twitter_link
    if linkedin_link := row.get("linkedinlink"): social_dict["linkedinLink"] = linkedin_link
    if instagram_link := row.get("instagramlink"): social_dict["instagramLink"] = instagram_link

    # ── Removed Confidence & Suspicious logic per user request ──

    return {
        "facility_id": source_row_id if source_row_id else str(uuid.uuid4()),
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
        "latitude": latitude,
        "longitude": longitude,
        "phone_numbers": phone_numbers or None,
        "email": email,
        "websites": websites or None,
        "social_links": social_dict or None,
        "officialWebsite": official_website,
        "year_established": _try_int(year_established),
        "accepts_volunteers": accepts_volunteers,
        "capacity": _try_int(capacity),
        "no_doctors": _try_int(no_doctors),
        "description": desc,
        "mission_statement": mission_statement,
        "affiliation_types": affiliation_types or None,
        "operator_type": operator_type,
        "facility_type": facility_type,
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
