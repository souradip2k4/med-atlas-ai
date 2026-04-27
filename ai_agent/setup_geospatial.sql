-- =========================================================================
-- Geospatial UC Function
-- med_atlas_ai_v2.default.find_facilities_nearby
-- =========================================================================
-- Finds healthcare facilities within a given radius of a reference point.
-- Uses Spark SQL ST_DistanceSpheroid for precise geodesic distance on WGS84.
--
-- Args: query_json — JSON string with:
--   ref_lat           DOUBLE  — latitude of the reference location (from LocationIQ geocoding)
--   ref_lon           DOUBLE  — longitude of the reference location (from LocationIQ geocoding)
--   radius_km         DOUBLE  — search radius in kilometres
--
-- Attribute-scope filters (all optional, applied ON TOP of distance filter):
--   operator_type     STRING  — 'private' | 'public'
--   organization_type STRING  — 'facility' | 'ngo'
--   facility_type     STRING  — 'hospital' | 'clinic' | 'dentist' | 'farmacy' | 'doctor'
--   affiliation_type  STRING  — value in affiliation_types array
-- NOTE: region and city are NOT accepted — geographic scoping is done via lat/lon + radius_km.
-- =========================================================================

CREATE OR REPLACE FUNCTION med_atlas_ai_v2.default.find_facilities_nearby(
  query_json STRING COMMENT 'JSON payload for geospatial lookup. Required keys: ref_lat (DOUBLE), ref_lon (DOUBLE), radius_km (DOUBLE). Optional attribute filters: operator_type, organization_type, facility_type, affiliation_type. Geographic scoping is done exclusively via lat/lon + radius_km — region and city are not accepted.'
)
RETURNS STRING
LANGUAGE SQL
COMMENT 'Returns up to 100 facilities sorted by ascending distance from a reference point using ST_DistanceSpheroid (WGS84 spheroid). The reference lat/lon must be pre-geocoded by the caller (e.g., via LocationIQ). Accepts optional attribute filters: organization_type, facility_type, operator_type, affiliation_type.'
RETURN (

  -- Extract all parameters as scalars once to avoid correlated subquery issues
  WITH global_params AS (
    SELECT
      CAST(parse_json(query_json):ref_lat           AS DOUBLE)  AS ref_lat,
      CAST(parse_json(query_json):ref_lon           AS DOUBLE)  AS ref_lon,
      CAST(parse_json(query_json):radius_km         AS DOUBLE)  AS radius_km,
      CAST(parse_json(query_json):operator_type     AS STRING)  AS f_operator_type,
      CAST(parse_json(query_json):organization_type AS STRING)  AS f_organization_type,
      CAST(parse_json(query_json):facility_type     AS STRING)  AS f_facility_type,
      CAST(parse_json(query_json):affiliation_type  AS STRING)  AS f_affiliation_type
  ),

  -- Attribute scope filter — all optional, NULL = no filter applied
  -- Geographic scoping is handled exclusively by ST_DistanceSpheroid + radius_km
  scoped_facilities AS (
    SELECT fr.*
    FROM med_atlas_ai.default.facility_records fr
    JOIN global_params gp ON (
      fr.latitude  IS NOT NULL
      AND fr.longitude IS NOT NULL
      AND (gp.f_operator_type     IS NULL OR LOWER(fr.operator_type)     = LOWER(gp.f_operator_type))
      AND (gp.f_organization_type IS NULL OR LOWER(fr.organization_type) = LOWER(gp.f_organization_type))
      AND (gp.f_facility_type     IS NULL OR LOWER(fr.facility_type)     = LOWER(gp.f_facility_type))
      AND (gp.f_affiliation_type  IS NULL OR ARRAY_CONTAINS(fr.affiliation_types, gp.f_affiliation_type))
    )
  ),

  sorted_facilities AS (
    SELECT
      sf.facility_id,
      sf.facility_name,
      sf.facility_type,
      sf.city,
      sf.state,
      sf.country,
      sf.latitude                              AS fac_lat,
      sf.longitude                             AS fac_lon,
      ARRAY_JOIN(COALESCE(sf.specialties, ARRAY()), ', ') AS specialties_str,
      ARRAY_JOIN(COALESCE(sf.procedures,  ARRAY()), '; ') AS procedures_str,
      ARRAY_JOIN(COALESCE(sf.equipment,   ARRAY()), '; ') AS equipment_str,
      ROUND(
        ST_DistanceSpheroid(
          ST_POINT(gp.ref_lon, gp.ref_lat),
          ST_POINT(sf.longitude, sf.latitude)
        ) / 1000.0,
        2
      )                                        AS distance_km,
      gp.radius_km                             AS r_km
    FROM scoped_facilities sf
    CROSS JOIN global_params gp
    ORDER BY distance_km
  ),
  
  filtered_and_limited AS (
    SELECT *
    FROM sorted_facilities
    WHERE distance_km <= r_km
    LIMIT 100
  )

  SELECT to_json(
    map_from_arrays(
      array('analysis_type', 'reference_lat', 'reference_lon', 'radius_km',
            'total_facilities_returned', 'facilities'),
      array(
        'nearby',
        CAST((SELECT ref_lat   FROM global_params) AS STRING),
        CAST((SELECT ref_lon   FROM global_params) AS STRING),
        CAST((SELECT radius_km FROM global_params) AS STRING),
        CAST((SELECT COUNT(*) FROM filtered_and_limited) AS STRING),
        to_json(collect_list(named_struct(
          'facility_id',   facility_id,
          'facility_name', facility_name,
          'facility_type', facility_type,
          'city',          city,
          'state',         state,
          'country',       country,
          'distance_km',   distance_km,
          'specialties',   specialties_str,
          'procedures',    procedures_str,
          'equipment',     equipment_str
        )))
      )
    )
  )
  FROM filtered_and_limited
);
