-- =========================================================================
-- Geospatial UC Function
-- med_atlas_ai.default.find_facilities_nearby
-- =========================================================================
-- Finds healthcare facilities within a given radius of a reference point.
-- Uses Spark SQL ST_DistanceSpheroid for precise geodesic distance on WGS84.
--
-- Args: query_json — JSON string with:
--   ref_lat           DOUBLE  — latitude of the reference location
--   ref_lon           DOUBLE  — longitude of the reference location
--   radius_km         DOUBLE  — search radius in kilometres
--   condition         STRING  (optional) — medical condition / procedure keyword filter
--   analysis_type     STRING  (optional) — "nearby" (default) | "cold_spot" | "urban_rural"
--
-- Scope filters (all optional):
--   operator_type     STRING  — 'private' | 'public'
--   organization_type STRING  — 'facility' | 'ngo'
--   facility_type     STRING  — 'hospital' | 'clinic' | 'dentist' | 'farmacy' | 'doctor'
--   affiliation_type  STRING  — value in affiliation_types array
--   region            STRING  — restrict to fr.state = region
--   city              STRING  — restrict to fr.city = city
-- =========================================================================

CREATE OR REPLACE FUNCTION med_atlas_ai_v2.default.find_facilities_nearby(
  query_json STRING
)
RETURNS STRING
LANGUAGE SQL
COMMENT 'Geospatial facility search using ST_DistanceSpheroid on WGS84 spheroid'
RETURN (

  -- Extract all parameters as scalars once to avoid correlated subquery issues
  WITH global_params AS (
    SELECT
      CAST(parse_json(query_json):ref_lat           AS DOUBLE)  AS ref_lat,
      CAST(parse_json(query_json):ref_lon           AS DOUBLE)  AS ref_lon,
      CAST(parse_json(query_json):radius_km         AS DOUBLE)  AS radius_km,
      COALESCE(CAST(parse_json(query_json):condition AS STRING), '') AS condition,
      COALESCE(CAST(parse_json(query_json):analysis_type AS STRING), 'nearby') AS analysis_type,
      CAST(parse_json(query_json):urban_hubs        AS STRING)  AS urban_hubs_json,
      -- Scope filter scalars
      CAST(parse_json(query_json):region            AS STRING)  AS f_region,
      CAST(parse_json(query_json):city              AS STRING)  AS f_city,
      CAST(parse_json(query_json):operator_type     AS STRING)  AS f_operator_type,
      CAST(parse_json(query_json):organization_type AS STRING)  AS f_organization_type,
      CAST(parse_json(query_json):facility_type     AS STRING)  AS f_facility_type,
      CAST(parse_json(query_json):affiliation_type  AS STRING)  AS f_affiliation_type
  ),

  -- Master scope filter — all optional, NULL = no filter applied
  scoped_facilities AS (
    SELECT fr.*
    FROM med_atlas_ai.default.facility_records fr
    JOIN global_params gp ON (
      fr.latitude  IS NOT NULL
      AND fr.longitude IS NOT NULL
      AND (gp.f_region IS NULL OR fr.state = gp.f_region)
      AND (gp.f_city IS NULL OR fr.city = gp.f_city)
      AND (gp.f_operator_type IS NULL OR LOWER(fr.operator_type) = LOWER(gp.f_operator_type))
      AND (gp.f_organization_type IS NULL OR LOWER(fr.organization_type) = LOWER(gp.f_organization_type))
      AND (gp.f_facility_type IS NULL OR LOWER(fr.facility_type) = LOWER(gp.f_facility_type))
      AND (gp.f_affiliation_type IS NULL OR ARRAY_CONTAINS(fr.affiliation_types, gp.f_affiliation_type))
    )
  )

  SELECT CASE

  -- ─── NEARBY: find all facilities within radius_km of a point ─────────────────
  WHEN (SELECT analysis_type FROM global_params) IN ('nearby', '')
  THEN (
    WITH sorted_facilities AS (
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
        ROUND(
          ST_DistanceSpheroid(
            ST_POINT(gp.ref_lon, gp.ref_lat),
            ST_POINT(sf.longitude, sf.latitude)
          ) / 1000.0,
          2
        )                                        AS distance_km,
        gp.condition                             AS cond,
        gp.radius_km                             AS r_km
      FROM scoped_facilities sf
      JOIN global_params gp ON (
        -- Optional condition filter
        gp.condition IS NULL
        OR gp.condition = ''
        OR EXISTS (
          SELECT 1
          FROM med_atlas_ai.default.facility_facts ff
          WHERE ff.facility_id = sf.facility_id
            AND LOWER(ff.fact_text) RLIKE LOWER(gp.condition)
        )
        OR EXISTS (
          SELECT 1
          FROM med_atlas_ai.default.facility_records fr2
          WHERE fr2.facility_id = sf.facility_id
            AND (
              ARRAY_CONTAINS(fr2.specialties, gp.condition)
              OR LOWER(ARRAY_JOIN(COALESCE(fr2.procedures, ARRAY()), ' ')) RLIKE LOWER(gp.condition)
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
          CAST((SELECT ref_lat   FROM global_params) AS STRING),
          CAST((SELECT ref_lon   FROM global_params) AS STRING),
          CAST((SELECT radius_km FROM global_params) AS STRING),
          CAST(COUNT(*) AS STRING),
          COALESCE((SELECT condition FROM global_params), 'none'),
          to_json(collect_list(named_struct(
            'facility_id',   facility_id,
            'facility_name', facility_name,
            'facility_type', facility_type,
            'city',          city,
            'state',         state,
            'country',       country,
            'distance_km',   distance_km,
            'specialties',   specialties_str,
            'procedures',    procedures_str
          )))
        )
      )
    )
    FROM sorted_facilities
    WHERE distance_km <= r_km
  )

  -- ─── COLD SPOT: find regions with zero matching facilities ────────────────────
  WHEN (SELECT analysis_type FROM global_params) = 'cold_spot'
  THEN (
    WITH region_facilities AS (
      SELECT
        sf.state,
        sf.country,
        AVG(sf.latitude)   AS region_lat,
        AVG(sf.longitude)  AS region_lon,
        COUNT(*)           AS total_facilities,
        SUM(CASE
          WHEN (
            gp.condition IS NULL
            OR gp.condition = ''
            OR EXISTS (
              SELECT 1
              FROM med_atlas_ai.default.facility_facts ff
              WHERE ff.facility_id = sf.facility_id
                AND LOWER(ff.fact_text) RLIKE LOWER(gp.condition)
            )
          ) THEN 1 ELSE 0
        END)               AS matching_facilities
      FROM scoped_facilities sf
      JOIN global_params gp ON sf.state IS NOT NULL
      GROUP BY sf.state, sf.country
      ORDER BY matching_facilities, sf.state
    )
    SELECT to_json(
      map_from_arrays(
        array('analysis_type', 'condition_filter', 'cold_spot_regions'),
        array(
          'cold_spot',
          COALESCE((SELECT condition FROM global_params), 'none'),
          to_json(collect_list(named_struct(
            'state',               state,
            'country',             country,
            'total_facilities',    total_facilities,
            'matching_facilities', matching_facilities,
            'is_cold_spot',        CAST(matching_facilities = 0 AS STRING)
          )))
        )
      )
    )
    FROM region_facilities
    WHERE matching_facilities = 0
  )

  -- ─── URBAN/RURAL GAP: distance to nearest urban hubs ─────────────────────────
  WHEN (SELECT analysis_type FROM global_params) = 'urban_rural'
  THEN (
    WITH hubs AS (
      SELECT
        CAST(hub.name AS STRING) AS hub_name,
        CAST(hub.lat  AS DOUBLE) AS hub_lat,
        CAST(hub.lon  AS DOUBLE) AS hub_lon
      FROM (
        SELECT explode(from_json(
          (SELECT urban_hubs_json FROM global_params),
          'array<struct<name:string,lat:double,lon:double>>'
        )) AS hub
      )
    ),
    facility_hub_distances AS (
      SELECT
        sf.facility_id,
        sf.facility_name,
        sf.facility_type,
        sf.city,
        sf.state,
        sf.latitude   AS fac_lat,
        sf.longitude  AS fac_lon,
        h.hub_name,
        (ST_DistanceSpheroid(ST_POINT(sf.longitude, sf.latitude), ST_POINT(h.hub_lon, h.hub_lat)) / 1000.0) AS dist_km
      FROM scoped_facilities sf
      JOIN global_params gp ON (
        gp.condition IS NULL
        OR gp.condition = ''
        OR EXISTS (
          SELECT 1 FROM med_atlas_ai.default.facility_facts ff
          WHERE ff.facility_id = sf.facility_id
            AND LOWER(ff.fact_text) RLIKE LOWER(gp.condition)
        )
      )
      CROSS JOIN hubs h
    ),
    ranked_distances AS (
      SELECT
        facility_id,
        facility_name,
        facility_type,
        city,
        state,
        fac_lat,
        fac_lon,
        hub_name,
        dist_km,
        ROW_NUMBER() OVER (PARTITION BY facility_id ORDER BY dist_km ASC) AS rnk
      FROM facility_hub_distances
    )
    SELECT to_json(
      map_from_arrays(
        array('analysis_type', 'condition_filter', 'facilities'),
        array(
          'urban_rural',
          COALESCE((SELECT condition FROM global_params), 'none'),
          to_json(collect_list(named_struct(
            'facility_id',            facility_id,
            'facility_name',          facility_name,
            'facility_type',          facility_type,
            'city',                   city,
            'state',                  state,
            'nearest_hub',            hub_name,
            'dist_to_nearest_hub_km', ROUND(dist_km, 2)
          )))
        )
      )
    )
    FROM ranked_distances
    WHERE rnk = 1
  )

  ELSE to_json(map_from_arrays(
    array('error'),
    array('Unknown analysis_type. Use: nearby | cold_spot | urban_rural')
  ))

  END
  FROM global_params
);