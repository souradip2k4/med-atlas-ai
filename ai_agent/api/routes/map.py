from fastapi import APIRouter
from ai_agent.api.schemas.map import MapSearchRequest, FilterMetadata, FacilityPoint, ExtractMapMarkersRequest, ExtractMapMarkersResponse
from ai_agent.api.services.databricks_sql import execute_sql
import os

CATALOG = os.environ.get("CATALOG")
SCHEMA = os.environ.get("SCHEMA")
TABLE_PREFIX = f"{CATALOG}.{SCHEMA}"
router = APIRouter(prefix="/map", tags=["Map"])

# Static metadata per user requirements
STATIC_AFFILIATIONS = [
    "academic", 
    "faith-tradition", 
    "government", 
    "community", 
    "philanthropy-legacy"
]
STATIC_FACILITIES = ["dentist", "hospital", "farmacy", "clinic", "doctor"]
STATIC_OPERATORS = ["private", "public"]
STATIC_ORGS = ["facility", "ngo"]

def _escape(val: str) -> str:
    """Safely escape single quotes for SQL."""
    if not val:
        return ""
    return val.replace("'", "''")

@router.get("/metadata", response_model=FilterMetadata)
def get_metadata():
    """Fetch distinct regions, cities, and specialties, plus return static filter lists."""
    query = f"SELECT DISTINCT state, city FROM {TABLE_PREFIX}.facility_records WHERE country = 'Ghana' AND state IS NOT NULL"
    results = execute_sql(query)
    
    cities_by_region = {}
    for row in results:
        state = row.get("state")
        city = row.get("city")
        if not state:
            continue
        if state not in cities_by_region:
            cities_by_region[state] = []
        if city and city not in cities_by_region[state]:
            cities_by_region[state].append(city)
            
    regions = list(cities_by_region.keys())
    
    # Fetch unique specialties
    spec_query = f"SELECT DISTINCT explode(specialties) AS specialty FROM {TABLE_PREFIX}.facility_records WHERE country = 'Ghana' AND specialties IS NOT NULL"
    spec_results = execute_sql(spec_query)
    specialties = sorted(
        [r.get("specialty") for r in spec_results if r.get("specialty")]
    )
    
    return FilterMetadata(
        regions=regions,
        cities_by_region=cities_by_region,
        specialties=specialties,
        affiliation_types=STATIC_AFFILIATIONS,
        facility_types=STATIC_FACILITIES,
        operator_types=STATIC_OPERATORS,
        organization_types=STATIC_ORGS
    )

@router.post("/search")
def search_facilities(request: MapSearchRequest):
    """
    Search facilities for the map using region and optional filters.
    
    Expected JSON Payload:
    {
        "region": "Greater Accra Region",          # Mandatory
        "city": "Accra",                           # Optional
        "specialties": ["Cardiology"],             # Optional array
        "facility_type": "hospital",               # Optional
        "operator_type": "public",                 # Optional
        "organization_type": "facility",           # Optional
        "affiliation_types": ["government"],       # Optional array
        "bbox": [5.5, -0.3, 5.7, -0.1]             # Optional [min_lat, min_lon, max_lat, max_lon]
    }
    """
    conditions = ["country = 'Ghana'"]
    
    # Mandatory Region
    conditions.append(f"state = '{_escape(request.region)}'")
    
    if request.city:
        conditions.append(f"city = '{_escape(request.city)}'")
    if request.facility_type:
        conditions.append(f"facility_type = '{_escape(request.facility_type)}'")
    if request.operator_type:
        conditions.append(f"operator_type = '{_escape(request.operator_type)}'")
    if request.organization_type:
        conditions.append(f"organization_type = '{_escape(request.organization_type)}'")
        
    if request.specialties and len(request.specialties) > 0:
        specs_str = ",".join(f"'{_escape(s)}'" for s in request.specialties)
        conditions.append(f"ARRAYS_OVERLAP(specialties, array({specs_str}))")
        
    if request.affiliation_types and len(request.affiliation_types) > 0:
        affils_str = ",".join(f"'{_escape(a)}'" for a in request.affiliation_types)
        conditions.append(f"ARRAYS_OVERLAP(affiliation_types, array({affils_str}))")
        
    # Viewport bounding box: [min_lat, min_lon, max_lat, max_lon]
    if request.bbox and len(request.bbox) == 4:
        min_lat, min_lon, max_lat, max_lon = request.bbox
        conditions.append(f"CAST(latitude AS DOUBLE) BETWEEN {min_lat} AND {max_lat}")
        conditions.append(f"CAST(longitude AS DOUBLE) BETWEEN {min_lon} AND {max_lon}")

    where_clause = " AND ".join(conditions)
    
    # Fetch specifically the fields required for the map UI pins and cards
    query = f"""
        SELECT 
            facility_id, facility_name, latitude, longitude, city, state, 
            year_established, facility_type, operator_type, organization_type, 
            affiliation_types, description
        FROM {TABLE_PREFIX}.facility_records
        WHERE {where_clause}
    """
    
    results = execute_sql(query)
    # The SDK parses doubles properly from unity catalog
    return {
        "count": len(results),
        "facilities": results
    }

@router.get("/facility/{identifier}")
def get_facility(identifier: str):
    """
    Fetch the full deep-dive profile of a single facility by ID or Name.
    
    Path Parameter:
        identifier: The UUID, unique identifier, or exact name of the facility.
        
    Example URL:
        GET /map/facility/fac-123-abc
        GET /map/facility/Korle-Bu%20Teaching%20Hospital
    """
    # Normalize extra spaces to exactly 1 space
    clean_identifier = " ".join(identifier.split())
    
    query = f"""
        SELECT *
        FROM {TABLE_PREFIX}.facility_records
        WHERE facility_id = '{_escape(clean_identifier)}'
           OR LOWER(facility_name) = LOWER('{_escape(clean_identifier)}')
        LIMIT 1
    """
    results = execute_sql(query)
    if not results:
        return {"error": "Facility not found"}
    return results[0]

@router.post("/extract-map-markers", response_model=ExtractMapMarkersResponse)
def extract_map_markers(request: ExtractMapMarkersRequest):
    """
    Extract facility names from the final markdown response and return their coordinates.
    
    Expected JSON Payload:
    {
        "markdown": "**Answer:** ... [final markdown string here]"
    }
    """
    import json
    from databricks_langchain import ChatDatabricks
    from langchain_core.messages import SystemMessage, HumanMessage
    from ai_agent.agent import LLM_ENDPOINT
    
    # Step 1: Use LLM to extract facility names from the markdown
    llm = ChatDatabricks(endpoint=LLM_ENDPOINT, temperature=0.0, max_tokens=2048)
    
    system_prompt = (
        "You are an expert data parser. Your exact task is to extract ALL specific medical facility names "
        "(hospitals, clinics, NGOs, etc.) explicitly mentioned in the provided markdown text. Look carefully inside markdown tables or lists.\n\n"
        "CRITICAL RULES:\n"
        "1. Extract the CLEAN facility name ONLY. Strip out any parenthetical tags, location names in brackets, "
        "facility type labels, IDs, duplicate warnings, or trailing dashes. Examples of cleaning:\n"
        "   - 'Eye Hospital In Accra (Accra) – hospital' -> Eye Hospital In Accra\n"
        "   - 'Bromley Park Dental Clinic (clinic)' -> Bromley Park Dental Clinic\n"
        "   - 'Chrispod Hospital & Diagnostic Center (duplicate entry)' -> Chrispod Hospital & Diagnostic Center\n"
        "   - 'Nunana Clinic (ID e58cd378-b972-4a1a-97d5-e55a3828dd76)' -> Nunana Clinic\n"
        "2. Do NOT extract standalone region/state/district names (e.g., do not extract just 'Greater Accra' or 'Ashanti' on their own). "
        "HOWEVER, if the region name is part of the actual facility name (e.g., 'Greater Accra Regional Hospital'), you MUST extract the full facility name.\n"
        "3. If there are NO specific facility names mentioned in the text, you MUST return the exact word: NONE\n\n"
        "Return ONLY a plain text list with one facility name per line. completely omit array brackets, commas, or quotes. Do not include markdown tags like ``` or any other conversational text."
    )
    
    messages = [
        SystemMessage(content=system_prompt),
        HumanMessage(content=request.markdown)
    ]
    
    response = llm.invoke(messages)
    
    # Handle both string and list content from AIMessage
    if isinstance(response.content, list):
        texts = []
        for c in response.content:
            if isinstance(c, dict):
                texts.append(c.get("text", ""))
            else:
                texts.append(str(c))
        raw_text = "".join(texts).strip()
    else:
        raw_text = str(response.content).strip()
    
    # Clean up potential markdown formatting in the LLM response
    if raw_text.startswith("```text"):
        raw_text = raw_text[7:]
    elif raw_text.startswith("```"):
        raw_text = raw_text[3:]
    if raw_text.endswith("```"):
        raw_text = raw_text[:-3]
    raw_text = raw_text.strip()
    
    if raw_text.upper() == "NONE" or not raw_text:
        names = []
    else:
        # Split by newlines, clean up bullet points or dashes if the LLM hallucinated them
        raw_lines = raw_text.split("\n")
        names = []
        for line in raw_lines:
            clean_line = line.strip().strip("-*\"',[] ").strip()
            # Try to safely decode weird unicode narrow-no-break spaces
            clean_line = clean_line.replace("\u202f", " ").replace("\u00a0", " ")
            if clean_line:
                names.append(clean_line)
    
    if not names:
        return ExtractMapMarkersResponse(
            map_markers=[], 
            extracted_names=[], 
            raw_sql_results=[]
        )
        
    # Step 2: Use SQL to fetch coordinates for the exact names via LIKE clause
    # Build dynamic OR clauses for each extracted name
    or_clauses = []
    for name in names:
        escaped_name = _escape(name).lower()
        or_clauses.append(f"LOWER(facility_name) LIKE '%{escaped_name}%'")
        
    or_clause_str = " OR ".join(or_clauses)
    
    query = f"""
        SELECT facility_id, facility_name, latitude, longitude
        FROM {TABLE_PREFIX}.facility_records
        WHERE organization_type IN ('facility', 'ngo')
          AND latitude IS NOT NULL
          AND longitude IS NOT NULL
          AND ({or_clause_str})
        LIMIT 50
    """
    
    results = execute_sql(query)
    
    markers = []
    # Deduplicate by facility_id to avoid multiple hits
    seen_ids = set()
    
    for row in results:
        f_id = row.get("facility_id")
        if f_id and f_id not in seen_ids:
            seen_ids.add(f_id)
            try:
                lat = float(row.get("latitude"))
                lng = float(row.get("longitude"))
                markers.append({
                    "id": f_id,
                    "name": row.get("facility_name"),
                    "latitude": lat,
                    "longitude": lng
                })
            except (ValueError, TypeError):
                continue
                
    return ExtractMapMarkersResponse(
        map_markers=markers,
        extracted_names=names,
        raw_sql_results=results
    )
