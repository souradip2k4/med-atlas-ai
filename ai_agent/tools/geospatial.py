
from langchain.tools import tool
import json
import requests
import time
from ai_agent.constants import GHANA_REGIONS
from ai_agent.config import GEOSPATIAL_UC_FUNCTION_NAME
from unitycatalog.ai.langchain.toolkit import UCFunctionToolkit
import os
    

# ─── Tool 4 — Geospatial Query ───────────────────────────────────────────────

@tool
def geospatial_query_tool(
    reference_location: str,
    radius_km: float = 50.0,
    facility_type: str | None = None,
    operator_type: str | None = None,
    organization_type: str | None = None,
    affiliation_type: str | None = None,
    scan_all_ghana_regions: bool = False,
) -> str:
    """
    Geospatial facility search using ST_DistanceSpheroid on the WGS84 spheroid.
    Geocodes `reference_location` via LocationIQ to obtain precise lat/lon,
    then queries the Unity Catalog SQL function for all facilities within radius_km.

    Returns: A list of up to 100 facilities within radius_km, sorted by ascending distance.

    Args:
        reference_location:       REQUIRED. Name of the city or region to center the search on
                                  (e.g., "Accra", "Kumasi", "Volta region"). The tool
                                  geocodes this automatically via LocationIQ — do NOT pass
                                  raw lat/lon coordinates.
        radius_km:                Search radius in kilometres (default 50).
        facility_type:            Optional. 'hospital' | 'clinic' | 'dentist' | 'farmacy' | 'doctor'.
                                  Only pass if the user explicitly mentioned this.
        operator_type:            Optional. 'private' | 'public'.
                                  Only pass if the user explicitly mentioned this.
        organization_type:        Optional. 'facility' | 'ngo'.
                                  Only pass if the user explicitly mentioned this.
        affiliation_type:         Optional. 'faith-tradition' | 'government' | 'community' |
                                  'philanthropy-legacy' | 'academic'.
                                  Only pass if the user explicitly mentioned this.
        scan_all_ghana_regions:   Set to True for global cold-spot analysis when the user
                                  asks about cold spots across ALL of Ghana without specifying
                                  a particular location. Geocodes each of the 16 Ghana
                                  regional capitals via LocationIQ and returns the
                                  deduplicated union of all facilities within radius_km of each.

    CRITICAL: NEVER pass ref_lat or ref_lon — those parameters no longer exist.
    Always pass `reference_location` as a plain string from the user’s prompt.

    Trigger keywords: "within", "km", "distance", "near", "nearby", "closest",
    "cold spot", "geographic", "radius", "proximity", "urban", "rural".
    """

    # ── GLOBAL SCAN MODE ─────────────────────────────────────────────────────────
    # When scan_all_ghana_regions=True, geocode all 16 Ghana regions and return
    # the deduplicated union of all facilities found within radius_km of each.
    if scan_all_ghana_regions:
        api_key = os.getenv("LOCATION_IQ_ACCESS_TOKEN")
        if not api_key:
            return "[Geospatial Query Error] LOCATION_IQ_ACCESS_TOKEN not set."

        all_facilities_by_id: dict[str, dict] = {}
        regions_successful: list[str] = []
        regions_failed: list[str] = []

        for _region in GHANA_REGIONS:
            try:
                _resp = requests.get(
                    "https://us1.locationiq.com/v1/search",
                    params={"key": api_key, "q": f"{_region} region, Ghana", "format": "json"},
                    timeout=5,
                )
                time.sleep(0.5)  # rate-limit guard: stay within 2 req/sec
                if _resp.status_code != 200 or not _resp.json():
                    regions_failed.append(_region)
                    continue
                _lat = float(_resp.json()[0]["lat"])
                _lon = float(_resp.json()[0]["lon"])
            except Exception:
                regions_failed.append(_region)
                continue

            _payload: dict = {"ref_lat": _lat, "ref_lon": _lon, "radius_km": radius_km}
            if operator_type:     _payload["operator_type"]     = operator_type
            if organization_type: _payload["organization_type"] = organization_type
            if facility_type:     _payload["facility_type"]     = facility_type
            if affiliation_type:  _payload["affiliation_type"]  = affiliation_type

            try:
                if not GEOSPATIAL_UC_FUNCTION_NAME:
                    continue
                _uc = UCFunctionToolkit(function_names=[GEOSPATIAL_UC_FUNCTION_NAME])
                _raw = _uc.tools[0].invoke({"query_json": json.dumps(_payload)})
                _outer = json.loads(_raw)
                if isinstance(_outer, dict) and "value" in _outer and "format" in _outer:
                    _inner = _outer["value"]
                    _outer = json.loads(_inner) if isinstance(_inner, str) else _inner
                _facs_raw = _outer.get("facilities", [])
                if isinstance(_facs_raw, str):
                    _facs_raw = json.loads(_facs_raw)
                for _fac in (_facs_raw if isinstance(_facs_raw, list) else []):
                    _fid = _fac.get("facility_id")
                    if _fid and _fid not in all_facilities_by_id:
                        all_facilities_by_id[_fid] = _fac
                regions_successful.append(_region)
            except Exception:
                regions_failed.append(_region)
                continue

        facilities_list = list(all_facilities_by_id.values())
        return json.dumps({
            "scan_type":                "all_ghana_regions",
            "radius_km":               radius_km,
            "regions_scanned":         regions_successful,
            "regions_failed":          regions_failed,
            "total_facilities_returned": len(facilities_list),
            "facilities":              facilities_list,
        }, indent=2)

    # ── SINGLE LOCATION MODE (geocoding always mandatory) ────────────────────────
    # Always geocode the reference_location string via LocationIQ.
    # The LLM must NEVER pass ref_lat/ref_lon directly.
    api_key = os.getenv("LOCATION_IQ_ACCESS_TOKEN")
    if not api_key:
        return "[Geospatial Query Error] LOCATION_IQ_ACCESS_TOKEN not set in environment."
    try:
        resp = requests.get(
            "https://us1.locationiq.com/v1/search",
            params={"key": api_key, "q": f"{reference_location}, Ghana", "format": "json"},
            timeout=5,
        )
        time.sleep(0.5)  # rate-limit guard: stay within 2 req/sec
        if resp.status_code == 200 and len(resp.json()) > 0:
            ref_lat = float(resp.json()[0]["lat"])
            ref_lon = float(resp.json()[0]["lon"])
        else:
            return f"[Geospatial Query Error] Could not dynamically geocode '{reference_location}'."
    except Exception as e:
        return f"[Geospatial Query Error] Geocoding failed: {e}"

    payload: dict = {
        "ref_lat":   ref_lat,
        "ref_lon":   ref_lon,
        "radius_km": radius_km,
    }
    # Attribute-level scope filters only (no city/region — geocoordinates handle geography)
    if facility_type:     payload["facility_type"]     = facility_type
    if operator_type:     payload["operator_type"]     = operator_type
    if organization_type: payload["organization_type"] = organization_type
    if affiliation_type:  payload["affiliation_type"]  = affiliation_type

    try:
        if not GEOSPATIAL_UC_FUNCTION_NAME:
            return (
                "[Geospatial Query Error] Missing UC function name. Set GEOSPATIAL_UC_FUNCTION_NAME "
                "or set CATALOG/SCHEMA for fallback resolution."
            )
        uc = UCFunctionToolkit(
            function_names=[GEOSPATIAL_UC_FUNCTION_NAME]
        )
        uc_fn = uc.tools[0]
        raw_result = uc_fn.invoke({"query_json": json.dumps(payload)})

        # UCFunctionToolkit wraps the return in {"format": "SCALAR", "value": "<json-string>"}.
        # Unwrap it so the LLM receives clean, well-formed JSON instead of an escaped string.
        outer = json.loads(raw_result)
        if isinstance(outer, dict) and "value" in outer and "format" in outer:
            inner = outer["value"]
            outer = json.loads(inner) if isinstance(inner, str) else inner

        # The SQL map_from_arrays forces all values to STRING (all values must share one type).
        # Rehydrate numeric metadata fields back to proper Python numbers.
        _float_fields = ("reference_lat", "reference_lon", "radius_km")
        _int_fields   = ("total_facilities_returned",)
        for f in _float_fields:
            if f in outer and isinstance(outer[f], str):
                try:
                    outer[f] = float(outer[f])
                except (ValueError, TypeError):
                    pass
        for f in _int_fields:
            if f in outer and isinstance(outer[f], str):
                try:
                    outer[f] = int(outer[f])
                except (ValueError, TypeError):
                    pass

        # The SQL function double-encodes the facilities array as a JSON string.
        # Parse it so the result is a proper nested object (not an escaped string).
        for key in ("facilities",):
            raw_val = outer.get(key)
            if isinstance(raw_val, str):
                try:
                    outer[key] = json.loads(raw_val)
                except (json.JSONDecodeError, TypeError):
                    pass  # leave as-is if not valid JSON

        return json.dumps(outer, indent=2)
    except Exception as exc:
        return f"[Geospatial Query Error] {exc}"
