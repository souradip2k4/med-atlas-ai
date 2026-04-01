from fastapi import APIRouter
from ai_agent.api.schemas.map import MapSearchRequest, FilterMetadata, FacilityPoint
from ai_agent.api.services.databricks_sql import execute_sql

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
    """Fetch distinct regions and cities, plus return static filter lists."""
    query = "SELECT DISTINCT state, city FROM med_atlas_ai.default.facility_records WHERE country = 'Ghana' AND state IS NOT NULL"
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
    
    return FilterMetadata(
        regions=regions,
        cities_by_region=cities_by_region,
        affiliation_types=STATIC_AFFILIATIONS,
        facility_types=STATIC_FACILITIES,
        operator_types=STATIC_OPERATORS,
        organization_types=STATIC_ORGS
    )

@router.post("/search")
def search_facilities(request: MapSearchRequest):
    """Search facilities for the map using region and optional filters."""
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

    where_clause = " AND ".join(conditions)
    
    # Fetch specifically the fields required for the map UI pins and cards
    query = f"""
        SELECT 
            facility_id, facility_name, latitude, longitude, city, state, 
            year_established, facility_type, operator_type, organization_type, 
            affiliation_types, description
        FROM med_atlas_ai.default.facility_records
        WHERE {where_clause}
    """
    
    results = execute_sql(query)
    # The SDK parses doubles properly from unity catalog
    return {"facilities": results}

@router.get("/facility/{facility_id}")
def get_facility(facility_id: str):
    """Fetch the full deep-dive profile of a single facility."""
    query = f"""
        SELECT *
        FROM med_atlas_ai.default.facility_records
        WHERE facility_id = '{_escape(facility_id)}'
        LIMIT 1
    """
    results = execute_sql(query)
    if not results:
        return {"error": "Facility not found"}
    return results[0]
