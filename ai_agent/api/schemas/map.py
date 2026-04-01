from pydantic import BaseModel, Field
from typing import List, Optional, Dict

class MapSearchRequest(BaseModel):
    region: str = Field(..., description="Mandatory region (state)")
    city: Optional[str] = None
    specialties: Optional[List[str]] = None
    facility_type: Optional[str] = None
    operator_type: Optional[str] = None
    organization_type: Optional[str] = None
    affiliation_types: Optional[List[str]] = None
    bbox: Optional[List[float]] = Field(
        default=None,
        description="Viewport bounding box: [min_lat, min_lon, max_lat, max_lon]. When provided, only facilities within this rectangle are returned."
    )

class FacilityPoint(BaseModel):
    facility_id: str
    facility_name: str
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    city: Optional[str] = None
    state: Optional[str] = None
    year_established: Optional[int] = None
    facility_type: Optional[str] = None
    operator_type: Optional[str] = None
    organization_type: Optional[str] = None
    affiliation_types: Optional[List[str]] = None
    description: Optional[str] = None

class FilterMetadata(BaseModel):
    regions: List[str]
    cities_by_region: Dict[str, List[str]]
    specialties: List[str]
    affiliation_types: List[str]
    facility_types: List[str]
    operator_types: List[str]
    organization_types: List[str]
