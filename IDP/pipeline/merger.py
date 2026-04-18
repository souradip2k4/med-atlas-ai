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
from pipeline.location_resolver import resolve_location as _resolve_location_via_gemini

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
  "accra": "Greater Accra",
  "dansoman": "Greater Accra",
  "accra newtown": "Greater Accra",
  "tema": "Greater Accra",
  "north legon": "Greater Accra",
  "klagon": "Greater Accra",
  "weija": "Greater Accra",
  "mataheko": "Greater Accra",
  "agbogbloshie": "Greater Accra",
  "osu": "Greater Accra",
  "tesano": "Greater Accra",
  "dzorwulu": "Greater Accra",
  "east legon": "Greater Accra",
  "agbogba": "Greater Accra",
  "dome": "Greater Accra",
  "nungua": "Greater Accra",
  "dodowa": "Greater Accra",
  "ashaiman": "Greater Accra",
  "osu – accra east": "Greater Accra",
  "oyarifa": "Greater Accra",
  "madina": "Greater Accra",
  "tema community 22": "Greater Accra",
  "accra central": "Greater Accra",
  "north kaneshie": "Greater Accra",
  "abelenkpe, accra": "Greater Accra",
  "achimota": "Greater Accra",
  "kasoa": "Central",
  "james town": "Greater Accra",
  "ridge": "Greater Accra",
  "kwashieman": "Greater Accra",
  "lapaz": "Greater Accra",
  "nima": "Greater Accra",
  "teshie": "Greater Accra",
  "darkuman-nyamekye": "Greater Accra",
  "mempeasem": "Greater Accra",
  "adenta-fafraha": "Greater Accra",
  "labadi": "Greater Accra",
  "amasaman": "Greater Accra",
  "cantonments": "Greater Accra",
  "adenta": "Greater Accra",
  "adentan": "Greater Accra",
  "haatso": "Greater Accra",
  "legon": "Greater Accra",
  "new weija": "Greater Accra",
  "ashale-botwe": "Greater Accra",
  "maamobi": "Greater Accra",
  "odorkor": "Greater Accra",
  "kordiabe": "Greater Accra",
  "adenta municipality": "Greater Accra",
  "new ashongman": "Greater Accra",
  "apremdo": "Western",
  "takoradi": "Western",
  "abura": "Western",
  "adjoum": "Western",
  "adum banso": "Western",
  "adumkrom": "Western",
  "nsuta": "Western",
  "daboase": "Western",
  "kamgbunli": "Western",
  "new takoradi": "Western",
  "asin": "Western",
  "ateiku": "Western",
  "axim": "Western",
  "benso": "Western",
  "ahimakrom": "Western",
  "brebre": "Western",
  "dompim": "Western",
  "sekondi": "Western",
  "elubo": "Western",
  "tarkwa": "Western",
  "asankrangua": "Western",
  "nsawura": "Western",
  "apowa": "Western",
  "mamudukrom": "Western",
  "kwesimintsim": "Western",
  "dixcove": "Western",
  "enyinabrim": "Western",
  "bakanta": "Western",
  "mangoase": "Western",
  "simpa": "Western",
  "bogoso": "Western",
  "eikwe": "Western",
  "oseikojokrom": "Western",
  "kojokrom/sekondi": "Western",
  "agona swfru": "Western",
  "aboadze": "Western",
  "manso amenfi": "Western",
  "acherensua": "Ahafo",
  "bechem": "Ahafo",
  "pafo nkwanta": "Ahafo",
  "goaso": "Ahafo",
  "duayaw nkwanta": "Ahafo",
  "abomosu": "Eastern",
  "nkawkaw": "Eastern",
  "akosombo": "Eastern",
  "koforidua": "Eastern",
  "nsawam": "Eastern",
  "odonkawkrom": "Eastern",
  "asamankese": "Eastern",
  "akwatia": "Eastern",
  "kwabeng": "Eastern",
  "mepom": "Eastern",
  "somanya": "Eastern",
  "new abirim": "Eastern",
  "adoagyiri-adeiso": "Eastern",
  "obosomase": "Eastern",
  "suhum": "Eastern",
  "akuapim-mampong": "Eastern",
  "abuakwa": "Ashanti",
  "ahodwo": "Ashanti",
  "kumasi": "Ashanti",
  "afamaso": "Ashanti",
  "agogo": "Ashanti",
  "atonsu kumasi": "Ashanti",
  "asokore": "Ashanti",
  "akrofrom": "Ashanti",
  "tepa": "Ashanti",
  "wamasi": "Ashanti",
  "nyinamponase": "Ashanti",
  "anyinasuso": "Ashanti",
  "anyinasusu": "Ashanti",
  "asamang": "Ashanti",
  "asuofia": "Ashanti",
  "akaporiso": "Ashanti",
  "obuasi": "Ashanti",
  "drobonso": "Ashanti",
  "apaaso": "Ashanti",
  "tikrom": "Ashanti",
  "ejisu": "Ashanti",
  "bekwai": "Ashanti",
  "sekyere": "Ashanti",
  "ejura": "Ashanti",
  "wiamoase": "Ashanti",
  "akwatialine": "Ashanti",
  "offinso": "Ashanti",
  "asokore mampong": "Ashanti",
  "juaben": "Ashanti",
  "juaso": "Ashanti",
  "buokrom": "Ashanti",
  "kokofu": "Ashanti",
  "kuntanase": "Ashanti",
  "kwadaso": "Ashanti",
  "kasei (via ejura)": "Ashanti",
  "mampong": "Ashanti",
  "mankranso": "Ashanti",
  "santasi": "Ashanti",
  "dompoase": "Ashanti",
  "nkenkaso": "Ashanti",
  "kawkawti": "Ashanti",
  "mpatuom": "Ashanti",
  "jamasi": "Ashanti",
  "anolga": "Ashanti",
  "jacobu": "Ashanti",
  "donyina": "Ashanti",
  "agroyesum": "Ashanti",
  "apinkra": "Ashanti",
  "pramso": "Ashanti",
  "pramiso": "Ashanti",
  "kumawu": "Ashanti",
  "tanoso": "Ashanti",
  "ajumako": "Central",
  "cape coast": "Central",
  "ankaful": "Central",
  "dunkwa-on-offin": "Central",
  "afransi": "Central",
  "mankessim": "Central",
  "twabidi": "Central",
  "breman asikuma": "Central",
  "ayanfuri": "Central",
  "dominase": "Central",
  "assin-foso": "Central",
  "gomaa buduburam": "Central",
  "apam": "Central",
  "agona swedru": "Central",
  "winneba": "Central",
  "adidome": "Volta",
  "akatsi": "Volta",
  "anfoega": "Volta",
  "battor": "Volta",
  "sogakope": "Volta",
  "aflao": "Volta",
  "kpando": "Volta",
  "ho": "Volta",
  "anloga": "Volta",
  "hohoe": "Volta",
  "keta": "Volta",
  "peki": "Volta",
  "weme – abor": "Volta",
  "nkwanta": "Oti",
  "dzodze": "Volta",
  "techiman": "Bono East",
  "atebubu": "Bono East",
  "kintampo": "Bono East",
  "yeji": "Bono East",
  "yabraso": "Bono East",
  "tamale": "Northern",
  "walewale": "Northern",
  "bimbilla": "Northern",
  "yendi": "Northern",
  "kpandai": "Northern",
  "karaga": "Northern",
  "yabologu": "Northern",
  "kparigu": "Northern",
  "tatale": "Northern",
  "tolon": "Northern",
  "zabzugu tatale": "Northern",
  "sefwi asawinso": "Western North",
  "bibiani": "Western North",
  "enchi": "Western North",
  "sefwi wiawso": "Western North",
  "juaboso": "Western North",
  "sefwi-asafo": "Western North",
  "nalerigu": "North East",
  "worawora": "Oti",
  "abesim - sunyani": "Bono",
  "abesim": "Bono",
  "berekum": "Bono",
  "sunyani": "Bono",
  "dormaa ahenkro": "Bono",
  "sromani": "Bono",
  "wenchi": "Bono",
  "apenkro": "Bono",
  "bole": "Savannah",
  "salaga": "Savannah",
  "damongo": "Savannah",
  "daffiama": "Upper West",
  "gwo": "Upper West",
  "nadawli": "Upper West",
  "wa": "Upper West",
  "wechiau": "Upper West"
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
    """Delete junk location/contact/directory strings using the shared _GARBAGE_KEYWORDS."""
    from pipeline.extractor import _GARBAGE_KEYWORDS
    if not arr:
        return []
    return [
        item for item in arr
        if not any(kw in item.lower() for kw in _GARBAGE_KEYWORDS)
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
    capabilities_from_llm = _clean_array(facts.capability if facts else None)
    capabilities = capabilities_from_llm if capabilities_from_llm else None

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
    # ── Location: city and state resolution cascade ─────────────────────────
    # Step 1: take values directly from CSV (fac is always None now)
    city = _first_non_null(
        fac.address_city if fac else None, row.get("address_city")
    )
    state = _first_non_null(
        fac.address_stateOrRegion if fac else None,
        row.get("address_stateorregion"),
    )
    country = _first_non_null(
        fac.address_country if fac else None, row.get("address_country")
    )
    country_code = _first_non_null(
        fac.address_countryCode if fac else None, row.get("address_countrycode")
    )

    # Step 2: if state is missing but city is present → dictionary lookup first
    if city and not state:
        state = _infer_ghana_region(city)

        # Step 3: dictionary missed → call Gemini (Case 1: city known, region missing)
        if not state:
            logger.info(
                "[Location] City '%s' not in dictionary for '%s' — calling Gemini.",
                city, facility_name,
            )
            geo_inferred = _resolve_location_via_gemini(
                facility_name=facility_name,
                city=city,
                address_line1=row.get("address_line1"),
                address_line2=row.get("address_line2"),
                address_line3=row.get("address_line3"),
                country=country or "Ghana",
            )
            state = geo_inferred.get("inferred_region") or None

    # Step 4: both city AND state are missing → skip dictionary, call Gemini directly (Case 2)
    elif not city and not state:
        logger.info(
            "[Location] Both city and state missing for '%s' — calling Gemini.",
            facility_name,
        )
        geo_inferred = _resolve_location_via_gemini(
            facility_name=facility_name,
            city=None,
            address_line1=row.get("address_line1"),
            address_line2=row.get("address_line2"),
            address_line3=row.get("address_line3"),
            country=country or "Ghana",
        )
        city = geo_inferred.get("inferred_city") or None
        state = geo_inferred.get("inferred_region") or None

    # Step 5: LocationIQ — lat/lon ONLY (city and state already fully resolved above)
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
