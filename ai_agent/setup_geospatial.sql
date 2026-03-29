-- =========================================================================
-- Geospatial UC Function
-- med_atlas_ai.default.find_facilities_nearby
-- =========================================================================
-- Finds healthcare facilities within a given radius of a reference point.
-- Uses Spark SQL ST_DistanceSpheroid for precise geodesic distance on the
-- WGS84 spheroid (sub-meter accuracy, much better than Haversine).
--
-- Schema dependencies:
--   facility_records: facility_id, facility_name, facility_type,
--     city, state, country, latitude, longitude,
--     specialties[], procedures[], equipment[], capabilities[]
--   facility_facts:   facility_id, fact_text, fact_type
--
-- Args: query_json — JSON string with:
--   ref_lat        DOUBLE  — latitude of the reference location
--   ref_lon        DOUBLE  — longitude of the reference location
--   radius_km      DOUBLE  — search radius in kilometres
--   condition      STRING  (optional) — medical condition / procedure keyword filter
--   analysis_type  STRING  (optional) — "nearby" (default) | "cold_spot" | "urban_rural"
--
-- Returns: JSON string with facilities array, sorted by distance ascending
-- =========================================================================

CREATE OR REPLACE FUNCTION med_atlas_ai.default.find_facilities_nearby(
  query_json STRING
)
RETURNS STRING
LANGUAGE SQL
COMMENT 'Geospatial facility search using ST_DistanceSpheroid on WGS84 spheroid'
RETURN (

  -- Determine analysis type
  CASE

  -- ─── NEARBY: find all facilities within radius_km of a point ─────────────────
  WHEN COALESCE(CAST(parse_json(query_json):analysis_type AS STRING), 'nearby') IN ('nearby', '')
  THEN (
    WITH params AS (
      SELECT 
        CAST(parse_json(query_json):ref_lat AS DOUBLE) AS ref_lat,
        CAST(parse_json(query_json):ref_lon AS DOUBLE) AS ref_lon,
        CAST(parse_json(query_json):radius_km AS DOUBLE) AS radius_km,
        COALESCE(CAST(parse_json(query_json):condition AS STRING), '') AS condition
    ),
    sorted_facilities AS (
      SELECT
        fr.facility_name,
        fr.facility_type,
        fr.city,
        fr.state,
        fr.country,
        fr.latitude                             AS fac_lat,
        fr.longitude                            AS fac_lon,
        ARRAY_JOIN(fr.specialties, ', ')        AS specialties_str,
        ARRAY_JOIN(fr.procedures,  '; ')        AS procedures_str,
        ROUND(
          ST_DistanceSpheroid(
            ST_POINT(p.ref_lon, p.ref_lat),
            ST_POINT(fr.longitude, fr.latitude)
          ) / 1000.0,   -- convert metres → km
          2
        )                                       AS distance_km
      FROM med_atlas_ai.default.facility_records fr
      CROSS JOIN params p
      WHERE
        fr.latitude  IS NOT NULL
        AND fr.longitude IS NOT NULL
        -- Optional condition filter: check facility_facts for a matching keyword
        AND (
          p.condition IS NULL
          OR p.condition = ''
          OR EXISTS (
            SELECT 1
            FROM med_atlas_ai.default.facility_facts ff
            WHERE ff.facility_id = fr.facility_id
              AND LOWER(ff.fact_text) RLIKE LOWER(p.condition)
          )
          OR EXISTS (
            SELECT 1
            FROM med_atlas_ai.default.facility_records fr2
            WHERE fr2.facility_id = fr.facility_id
              AND (
                ARRAY_CONTAINS(fr2.specialties, p.condition)
                OR LOWER(ARRAY_JOIN(fr2.procedures, ' ')) RLIKE LOWER(p.condition)
              )
          )
        )
      ORDER BY distance_km
    )
    SELECT to_json(
      map_from_arrays(
        array('analysis_type', 'reference_lat', 'reference_lon', 'radius_km',
              'total_facilities_found', 'condition_filter', 'facilities'),
        array(
          'nearby',
          CAST((SELECT ref_lat FROM params) AS STRING),
          CAST((SELECT ref_lon FROM params) AS STRING),
          CAST((SELECT radius_km FROM params) AS STRING),
          CAST(COUNT(*) AS STRING),
          COALESCE((SELECT condition FROM params), 'none'),
          to_json(collect_list(named_struct(
            'facility_name',   facility_name,
            'facility_type',   facility_type,
            'city',            city,
            'state',           state,
            'country',         country,
            'distance_km',     distance_km,
            'latitude',        fac_lat,
            'longitude',       fac_lon,
            'specialties',     specialties_str,
            'procedures',      procedures_str
          )))
        )
      )
    )
    FROM sorted_facilities
    CROSS JOIN params
    WHERE distance_km <= params.radius_km
  )

  -- ─── COLD SPOT: find regions with zero matching facilities nearby ─────────────
  -- Groups facilities by state (region), flags regions with no matching service
  WHEN CAST(parse_json(query_json):analysis_type AS STRING) = 'cold_spot'
  THEN (
    WITH params AS (
      SELECT COALESCE(CAST(parse_json(query_json):condition AS STRING), '') AS condition
    ),
    region_facilities AS (
      SELECT
        fr.state,
        fr.country,
        -- Representative centre per region: avg lat/lon of facilities in that region
        AVG(fr.latitude)  AS region_lat,
        AVG(fr.longitude) AS region_lon,
        COUNT(*)          AS total_facilities,
        SUM(CASE
          WHEN (
            p.condition IS NULL
            OR p.condition = ''
            OR EXISTS (
              SELECT 1
              FROM med_atlas_ai.default.facility_facts ff
              WHERE ff.facility_id = fr.facility_id
                AND LOWER(ff.fact_text) RLIKE LOWER(p.condition)
            )
          ) THEN 1 ELSE 0
        END)              AS matching_facilities
      FROM med_atlas_ai.default.facility_records fr
      CROSS JOIN params p
      WHERE fr.latitude IS NOT NULL
        AND fr.longitude IS NOT NULL
      GROUP BY fr.state, fr.country
      ORDER BY matching_facilities, state
    )
    SELECT to_json(
      map_from_arrays(
        array('analysis_type', 'condition_filter', 'cold_spot_regions'),
        array(
          'cold_spot',
          COALESCE((SELECT condition FROM params), 'none'),
          to_json(collect_list(named_struct(
            'state',                 state,
            'country',               country,
            'region_centre_lat',     region_lat,
            'region_centre_lon',     region_lon,
            'total_facilities',      total_facilities,
            'matching_facilities',   matching_facilities,
            'is_cold_spot',          CAST(matching_facilities = 0 AS STRING)
          )))
        )
      )
    )
    FROM region_facilities
    CROSS JOIN params
    WHERE matching_facilities = 0   -- only return actual cold spots
  )

  -- ─── URBAN/RURAL GAP: dynamically calculate distance to nearest provided hubs ─────────────────
  WHEN CAST(parse_json(query_json):analysis_type AS STRING) = 'urban_rural'
  THEN (
    WITH params AS (
      SELECT COALESCE(CAST(parse_json(query_json):condition AS STRING), '') AS condition
    ),
    hubs AS (
      SELECT
        CAST(hub.name AS STRING) AS hub_name,
        CAST(hub.lat AS DOUBLE) AS hub_lat,
        CAST(hub.lon AS DOUBLE) AS hub_lon
      FROM (
        SELECT explode(from_json(
          CAST(parse_json(query_json):urban_hubs AS STRING),
          'array<struct<name:string,lat:double,lon:double>>'
        )) AS hub
      )
    ),
    facility_hub_distances AS (
      SELECT
        fr.facility_id,
        fr.facility_name,
        fr.facility_type,
        fr.city,
        fr.state,
        h.hub_name,
        (ST_DistanceSpheroid(ST_POINT(fr.longitude, fr.latitude), ST_POINT(h.hub_lon, h.hub_lat)) / 1000.0) AS dist_km
      FROM med_atlas_ai.default.facility_records fr
      CROSS JOIN hubs h
      CROSS JOIN params p
      WHERE fr.latitude IS NOT NULL
        AND fr.longitude IS NOT NULL
        AND (
          p.condition IS NULL
          OR p.condition = ''
          OR EXISTS (
            SELECT 1 FROM med_atlas_ai.default.facility_facts ff
            WHERE ff.facility_id = fr.facility_id
              AND LOWER(ff.fact_text) RLIKE LOWER(p.condition)
          )
        )
    ),
    ranked_distances AS (
      SELECT
        facility_name,
        facility_type,
        city,
        state,
        hub_name,
        dist_km,
        ROW_NUMBER() OVER (PARTITION BY facility_id ORDER BY dist_km ASC) as rnk
      FROM facility_hub_distances
      ORDER BY dist_km
    )
    SELECT to_json(
      map_from_arrays(
        array('analysis_type', 'condition_filter', 'facilities'),
        array(
          'urban_rural',
          COALESCE((SELECT condition FROM params), 'none'),
          to_json(collect_list(named_struct(
            'facility_name', facility_name,
            'facility_type', facility_type,
            'city',          city,
            'state',         state,
            'nearest_hub',   hub_name,
            'dist_to_nearest_hub_km', ROUND(dist_km, 2)
          )))
        )
      )
    )
    FROM ranked_distances
    CROSS JOIN params
    WHERE rnk = 1
  )

  ELSE to_json(map_from_arrays(
    array('error'),
    array('Unknown analysis_type. Use: nearby | cold_spot | urban_rural')
  ))

  END
);