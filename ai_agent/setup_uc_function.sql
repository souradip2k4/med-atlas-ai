
-- =========================================================================
-- analyze_medical_query UC Function
-- med_atlas_ai.default.analyze_medical_query
-- =========================================================================
-- Routes to one of 7 analysis branches based on the query string.
-- All branches support optional scope filters via query_json parameters:
--   region, city, facility_id, facility_name, facility_ids (JSON array),
--   operator_type, organization_type, facility_type, affiliation_type
--
-- DATABRICKS PATTERN: parse_json inlined per-branch in WHERE clauses.
-- Shared CTE for scope is intentionally avoided because correlated subqueries
-- inside GROUP BY / aggregate functions are rejected by Spark SQL.
-- =========================================================================

CREATE OR REPLACE FUNCTION med_atlas_ai.default.analyze_medical_query(
  query_json STRING
)
RETURNS STRING
LANGUAGE SQL
COMMENT 'Medical anomaly detection and analytics with scope filtering'
RETURN (

  WITH query_params AS (
    SELECT
      parse_json(query_json)              AS parsed_input,
      LOWER(parse_json(query_json):query) AS query_lower,
      CAST(parse_json(query_json):region AS STRING) AS region_filter,
      CAST(parse_json(query_json):city AS STRING) AS city_filter,
      CAST(parse_json(query_json):facility_id AS STRING) AS facility_id_filter,
      CAST(parse_json(query_json):facility_name AS STRING) AS facility_name_filter,
      CAST(parse_json(query_json):operator_type AS STRING) AS operator_type_filter,
      CAST(parse_json(query_json):organization_type AS STRING) AS organization_type_filter,
      CAST(parse_json(query_json):facility_type AS STRING) AS facility_type_filter,
      CAST(parse_json(query_json):affiliation_type AS STRING) AS affiliation_type_filter,
      CAST(parse_json(query_json):facility_ids AS STRING) AS facility_ids_filter
  )

  SELECT CASE

  -- ══════════════════════════════════════════════════════════════════════════
  -- Branch 1. Unmet Needs / Regional Gaps
  -- Scope: region, city, operator_type, organization_type, facility_type,
  --        affiliation_type, facility_ids are ALL applied inline.
  -- global_specialties always scans ALL facilities (reference universe).
  -- ══════════════════════════════════════════════════════════════════════════
  WHEN (SELECT query_lower FROM query_params) RLIKE 'unmet|gap|need|service gap'
  THEN (
    WITH
    global_specialties AS (
      SELECT COLLECT_SET(specialty) AS all_known_specialties
      FROM (
        SELECT EXPLODE(COALESCE(specialties, ARRAY())) AS specialty
        FROM med_atlas_ai.default.facility_records
        WHERE organization_type = 'facility'
      )
      WHERE specialty IS NOT NULL AND TRIM(specialty) != ''
    ),
    region_coverage AS (
      SELECT
        fr.state                                                              AS region,
        COUNT(DISTINCT fr.facility_id)                                        AS total_facilities,
        COLLECT_SET(specialty_val)                                            AS region_specialties,
        FLATTEN(COLLECT_LIST(COALESCE(fr.procedures, ARRAY())))              AS all_procedures,
        FLATTEN(COLLECT_LIST(COALESCE(fr.equipment,  ARRAY())))              AS all_equipment
      FROM med_atlas_ai.default.facility_records fr
      LATERAL VIEW OUTER EXPLODE(COALESCE(fr.specialties, ARRAY())) AS specialty_val
      WHERE fr.state IS NOT NULL
        AND fr.organization_type = 'facility'
        AND ((SELECT region_filter FROM query_params) IS NULL
             OR fr.state = (SELECT region_filter FROM query_params))
        AND ((SELECT city_filter FROM query_params) IS NULL
             OR fr.city = (SELECT city_filter FROM query_params))
        AND ((SELECT operator_type_filter FROM query_params) IS NULL
             OR LOWER(fr.operator_type) = LOWER((SELECT operator_type_filter FROM query_params)))
        AND ((SELECT organization_type_filter FROM query_params) IS NULL
             OR LOWER(fr.organization_type) = LOWER((SELECT organization_type_filter FROM query_params)))
        AND ((SELECT facility_type_filter FROM query_params) IS NULL
             OR LOWER(fr.facility_type) = LOWER((SELECT facility_type_filter FROM query_params)))
        AND ((SELECT affiliation_type_filter FROM query_params) IS NULL
             OR ARRAY_CONTAINS(fr.affiliation_types, (SELECT affiliation_type_filter FROM query_params)))
        AND ((SELECT facility_ids_filter FROM query_params) IS NULL
             OR ARRAY_CONTAINS(from_json((SELECT facility_ids_filter FROM query_params), 'ARRAY<STRING>'), fr.facility_id))
      GROUP BY fr.state
    )
    SELECT to_json(
      map_from_arrays(
        array('query', 'findings'),
        array(
          (SELECT parsed_input:query FROM query_params),
          to_json(array_agg(named_struct(
            'type',                'regional_coverage',
            'region',              rc.region,
            'total_facilities',    rc.total_facilities,
            'specialties_present', rc.region_specialties,
            'specialties_missing', ARRAY_EXCEPT(gs.all_known_specialties, rc.region_specialties),
            'procedures_present',  rc.all_procedures,
            'equipment_present',   rc.all_equipment,
            'note', 'specialties_missing is a definitive SQL-computed list of specialties absent from this scope — report these as confirmed gaps. For procedures and equipment (free-text), apply medical domain knowledge to identify what is typically needed.'
          )))
        )
      )
    )
    FROM region_coverage rc
    CROSS JOIN global_specialties gs
  )



  -- ══════════════════════════════════════════════════════════════════════════
  -- Branch 2. Anomaly Flagging (capacity/doctor outliers)
  -- OPTION A — Global Baseline / Local Alerts:
  --   cap_stats   → ENTIRE dataset (global norms, never filtered)
  --   data_context + outliers → scoped by all filters inline
  -- ══════════════════════════════════════════════════════════════════════════
  WHEN (SELECT query_lower FROM query_params) RLIKE 'outlier|anomal|flag|unusual|inconsisten|signal'
   AND (SELECT query_lower FROM query_params) NOT RLIKE 'procedure|equipment|specialty|mismatch|valid|infrastr'
  THEN (
    WITH
    cap_stats AS (
      SELECT
        AVG(CAST(capacity   AS DOUBLE)) AS m_cap,
        STDDEV(CAST(capacity   AS DOUBLE)) AS s_cap,
        AVG(CAST(no_doctors AS DOUBLE)) AS m_docs,
        STDDEV(CAST(no_doctors AS DOUBLE)) AS s_docs
      FROM med_atlas_ai.default.facility_records
      WHERE organization_type = 'facility'
        AND capacity   IS NOT NULL
        AND no_doctors IS NOT NULL
    ),
    data_context AS (
      SELECT
        COUNT(*)                                                         AS total_facilities,
        COUNT(capacity)                                                  AS has_capacity,
        COUNT(no_doctors)                                                AS has_doctors,
        COUNT(*) - COUNT(capacity)                                       AS missing_capacity,
        COUNT(*) - COUNT(no_doctors)                                     AS missing_doctors,
        ROUND(100.0 * COUNT(capacity)   / NULLIF(COUNT(*), 0), 1)       AS pct_capacity_known,
        ROUND(100.0 * COUNT(no_doctors) / NULLIF(COUNT(*), 0), 1)       AS pct_doctors_known
      FROM med_atlas_ai.default.facility_records
      WHERE organization_type = 'facility'
        AND ((SELECT region_filter FROM query_params) IS NULL
             OR state = (SELECT region_filter FROM query_params))
        AND ((SELECT city_filter FROM query_params) IS NULL
             OR city = (SELECT city_filter FROM query_params))
        AND ((SELECT operator_type_filter FROM query_params) IS NULL
             OR LOWER(operator_type) = LOWER((SELECT operator_type_filter FROM query_params)))
        AND ((SELECT facility_type_filter FROM query_params) IS NULL
             OR LOWER(facility_type) = LOWER((SELECT facility_type_filter FROM query_params)))
        AND ((SELECT affiliation_type_filter FROM query_params) IS NULL
             OR ARRAY_CONTAINS(affiliation_types, (SELECT affiliation_type_filter FROM query_params)))
        AND ((SELECT facility_ids_filter FROM query_params) IS NULL
             OR ARRAY_CONTAINS(from_json((SELECT facility_ids_filter FROM query_params), 'ARRAY<STRING>'), facility_id))
    ),
    outliers AS (
      SELECT
        fr.facility_id,
        fr.facility_name,
        fr.state,
        fr.city,
        fr.latitude,
        fr.longitude,
        COALESCE(fr.facility_type, 'unknown')   AS facility_type,
        'beds'                                   AS measurement,
        CAST(fr.capacity AS INT)                 AS reported_value,
        ROUND(cs.m_cap, 0)                       AS typical_value,
        'unusual_value'                          AS flag_type,
        'This facility reports ' || fr.capacity || ' beds. The national average is around '
          || CAST(ROUND(cs.m_cap, 0) AS STRING)
          || ' beds — significantly outside the normal range.'  AS plain_reason
      FROM med_atlas_ai.default.facility_records fr
      CROSS JOIN cap_stats cs
      WHERE fr.organization_type = 'facility'
        AND fr.capacity IS NOT NULL AND CAST(fr.capacity AS INT) > 0
        AND cs.m_cap IS NOT NULL AND cs.s_cap IS NOT NULL
        AND (CAST(fr.capacity AS DOUBLE) < cs.m_cap - 3 * cs.s_cap
             OR CAST(fr.capacity AS DOUBLE) > cs.m_cap + 3 * cs.s_cap)
        AND ((SELECT region_filter FROM query_params) IS NULL
             OR fr.state = (SELECT region_filter FROM query_params))
        AND ((SELECT city_filter FROM query_params) IS NULL
             OR fr.city = (SELECT city_filter FROM query_params))
        AND ((SELECT operator_type_filter FROM query_params) IS NULL
             OR LOWER(fr.operator_type) = LOWER((SELECT operator_type_filter FROM query_params)))
        AND ((SELECT facility_type_filter FROM query_params) IS NULL
             OR LOWER(fr.facility_type) = LOWER((SELECT facility_type_filter FROM query_params)))
        AND ((SELECT affiliation_type_filter FROM query_params) IS NULL
             OR ARRAY_CONTAINS(fr.affiliation_types, (SELECT affiliation_type_filter FROM query_params)))
        AND ((SELECT facility_ids_filter FROM query_params) IS NULL
             OR ARRAY_CONTAINS(from_json((SELECT facility_ids_filter FROM query_params), 'ARRAY<STRING>'), fr.facility_id))

      UNION ALL

      SELECT
        fr.facility_id,
        fr.facility_name,
        fr.state,
        fr.city,
        fr.latitude,
        fr.longitude,
        COALESCE(fr.facility_type, 'unknown')   AS facility_type,
        'doctors'                                AS measurement,
        CAST(fr.no_doctors AS INT)               AS reported_value,
        ROUND(cs.m_docs, 0)                      AS typical_value,
        'unusual_value'                          AS flag_type,
        'This facility reports ' || fr.no_doctors || ' doctors. The national average is around '
          || CAST(ROUND(cs.m_docs, 0) AS STRING)
          || ' doctors — significantly outside the normal range.'  AS plain_reason
      FROM med_atlas_ai.default.facility_records fr
      CROSS JOIN cap_stats cs
      WHERE fr.organization_type = 'facility'
        AND fr.no_doctors IS NOT NULL AND CAST(fr.no_doctors AS INT) > 0
        AND cs.m_docs IS NOT NULL AND cs.s_docs IS NOT NULL
        AND (CAST(fr.no_doctors AS DOUBLE) < cs.m_docs - 3 * cs.s_docs
             OR CAST(fr.no_doctors AS DOUBLE) > cs.m_docs + 3 * cs.s_docs)
        AND ((SELECT region_filter FROM query_params) IS NULL
             OR fr.state = (SELECT region_filter FROM query_params))
        AND ((SELECT city_filter FROM query_params) IS NULL
             OR fr.city = (SELECT city_filter FROM query_params))
        AND ((SELECT operator_type_filter FROM query_params) IS NULL
             OR LOWER(fr.operator_type) = LOWER((SELECT operator_type_filter FROM query_params)))
        AND ((SELECT facility_type_filter FROM query_params) IS NULL
             OR LOWER(fr.facility_type) = LOWER((SELECT facility_type_filter FROM query_params)))
        AND ((SELECT affiliation_type_filter FROM query_params) IS NULL
             OR ARRAY_CONTAINS(fr.affiliation_types, (SELECT affiliation_type_filter FROM query_params)))
        AND ((SELECT facility_ids_filter FROM query_params) IS NULL
             OR ARRAY_CONTAINS(from_json((SELECT facility_ids_filter FROM query_params), 'ARRAY<STRING>'), fr.facility_id))
    )
    SELECT to_json(
      map_from_arrays(
        array('query', 'findings', 'data_coverage_summary'),
        array(
          (SELECT parsed_input:query FROM query_params),
          COALESCE(
            to_json(array_agg(named_struct(
              'type',           'anomaly_flagging',
              'facility_id',    facility_id,
              'facility_name',  facility_name,
              'state',          state,
              'city',           city,
              'facility_type',  facility_type,
              'measurement',    measurement,
              'reported_value', reported_value,
              'typical_value',  typical_value,
              'flag_type',      flag_type,
              'reason',         plain_reason
            ))),
            '[]'
          ),
          (SELECT to_json(named_struct(
            'total_facilities',       total_facilities,
            'beds_data_known_for',    has_capacity,
            'beds_data_missing',      missing_capacity,
            'beds_coverage_pct',      pct_capacity_known,
            'doctors_data_known_for', has_doctors,
            'doctors_data_missing',   missing_doctors,
            'doctors_coverage_pct',   pct_doctors_known,
            'note', 'IMPORTANT: Always start your response by telling the user how many facilities have unknown bed/doctor data. These facilities could not be checked for anomalies.'
          )) FROM data_context)
        )
      )
    )
    FROM (SELECT * FROM outliers ORDER BY ABS(reported_value - typical_value) DESC LIMIT 50)
  )

  -- ══════════════════════════════════════════════════════════════════════════
  -- Branch 3. NGO Overlap (scoped inline)
  -- ══════════════════════════════════════════════════════════════════════════
  WHEN (SELECT query_lower FROM query_params) RLIKE 'ngo overlap|overlapping ngo|same ngo|same region'
  THEN (
    WITH ngo_by_affiliation AS (
      SELECT
        fr.state AS region,
        fr.city,
        ARRAY_JOIN(COALESCE(fr.affiliation_types, ARRAY()), ',') AS affiliation_key,
        COUNT(DISTINCT fr.facility_id) AS n_facilities,
        COLLECT_LIST(fr.facility_name) AS facility_list
      FROM med_atlas_ai.default.facility_records fr
      WHERE fr.organization_type = 'ngo'
        AND fr.state IS NOT NULL
        AND fr.affiliation_types IS NOT NULL
        AND ((SELECT region_filter FROM query_params) IS NULL
             OR fr.state = (SELECT region_filter FROM query_params))
        AND ((SELECT city_filter FROM query_params) IS NULL
             OR fr.city = (SELECT city_filter FROM query_params))
        AND ((SELECT affiliation_type_filter FROM query_params) IS NULL
             OR ARRAY_CONTAINS(fr.affiliation_types, (SELECT affiliation_type_filter FROM query_params)))
        AND ((SELECT facility_ids_filter FROM query_params) IS NULL
             OR ARRAY_CONTAINS(from_json((SELECT facility_ids_filter FROM query_params), 'ARRAY<STRING>'), fr.facility_id))
      GROUP BY fr.state, fr.city, ARRAY_JOIN(COALESCE(fr.affiliation_types, ARRAY()), ',')
      HAVING COUNT(DISTINCT fr.facility_id) > 1
    )
    SELECT to_json(
      map_from_arrays(
        array('query', 'findings'),
        array(
          (SELECT parsed_input:query FROM query_params),
          to_json(array_agg(named_struct(
            'type',            'ngo_overlap_raw',
            'region',          region,
            'city',            city,
            'affiliation_key', affiliation_key,
            'n_facilities',    n_facilities,
            'facilities',      facility_list,
            'note',            'Determine if these represent duplicated efforts or complementary services'
          )))
        )
      )
    )
    FROM ngo_by_affiliation
  )

  -- ══════════════════════════════════════════════════════════════════════════
  -- Branch 4. Problem Type Classification (scoped inline)
  -- ══════════════════════════════════════════════════════════════════════════
  WHEN (SELECT query_lower FROM query_params) RLIKE 'problem type|root cause|gap type|classify gap|workforce|staffing|equipment gap|staff shortage'
  THEN (
    WITH
    data_context AS (
      SELECT
        COUNT(facility_id)                                                           AS total_facilities,
        COUNT(equipment)                                                             AS has_equipment,
        COUNT(specialties)                                                           AS has_specialties,
        COUNT(procedures)                                                            AS has_procedures,
        ROUND(100.0 * COUNT(equipment)   / NULLIF(COUNT(facility_id), 0), 1)        AS pct_equipment_known,
        ROUND(100.0 * COUNT(specialties) / NULLIF(COUNT(facility_id), 0), 1)        AS pct_specialties_known,
        ROUND(100.0 * COUNT(procedures)  / NULLIF(COUNT(facility_id), 0), 1)        AS pct_procedures_known
      FROM med_atlas_ai.default.facility_records
      WHERE organization_type = 'facility'
        AND ((SELECT region_filter FROM query_params) IS NULL
             OR state = (SELECT region_filter FROM query_params))
        AND ((SELECT city_filter FROM query_params) IS NULL
             OR city = (SELECT city_filter FROM query_params))
        AND ((SELECT operator_type_filter FROM query_params) IS NULL
             OR LOWER(operator_type) = LOWER((SELECT operator_type_filter FROM query_params)))
        AND ((SELECT organization_type_filter FROM query_params) IS NULL
             OR LOWER(organization_type) = LOWER((SELECT organization_type_filter FROM query_params)))
        AND ((SELECT facility_type_filter FROM query_params) IS NULL
             OR LOWER(facility_type) = LOWER((SELECT facility_type_filter FROM query_params)))
        AND ((SELECT affiliation_type_filter FROM query_params) IS NULL
             OR ARRAY_CONTAINS(affiliation_types, (SELECT affiliation_type_filter FROM query_params)))
        AND ((SELECT facility_ids_filter FROM query_params) IS NULL
             OR ARRAY_CONTAINS(from_json((SELECT facility_ids_filter FROM query_params), 'ARRAY<STRING>'), facility_id))
    ),
    gap_analysis AS (
      SELECT
        fr.facility_id,
        fr.facility_name,
        fr.state,
        fr.city,
        fr.latitude,
        fr.longitude,
        COALESCE(fr.facility_type, 'unknown') AS facility_type,
        fr.operator_type,
        COALESCE(CAST(fr.capacity AS INT), 0)    AS capacity,
        COALESCE(CAST(fr.no_doctors AS INT), 0)  AS no_doctors,
        COALESCE(size(fr.equipment), 0)          AS n_equip,
        (fr.equipment IS NULL)                   AS equipment_missing,
        COALESCE(size(fr.specialties), 0)        AS n_specialties,
        (fr.specialties IS NULL)                 AS specialties_missing,
        COALESCE(size(fr.procedures), 0)         AS n_procedures,
        (fr.procedures IS NULL)                  AS procedures_missing
      FROM med_atlas_ai.default.facility_records fr
      WHERE fr.organization_type = 'facility'
        AND ((SELECT region_filter FROM query_params) IS NULL
             OR fr.state = (SELECT region_filter FROM query_params))
        AND ((SELECT city_filter FROM query_params) IS NULL
             OR fr.city = (SELECT city_filter FROM query_params))
        AND ((SELECT operator_type_filter FROM query_params) IS NULL
             OR LOWER(fr.operator_type) = LOWER((SELECT operator_type_filter FROM query_params)))
        AND ((SELECT organization_type_filter FROM query_params) IS NULL
             OR LOWER(fr.organization_type) = LOWER((SELECT organization_type_filter FROM query_params)))
        AND ((SELECT facility_type_filter FROM query_params) IS NULL
             OR LOWER(fr.facility_type) = LOWER((SELECT facility_type_filter FROM query_params)))
        AND ((SELECT affiliation_type_filter FROM query_params) IS NULL
             OR ARRAY_CONTAINS(fr.affiliation_types, (SELECT affiliation_type_filter FROM query_params)))
        AND ((SELECT facility_ids_filter FROM query_params) IS NULL
             OR ARRAY_CONTAINS(from_json((SELECT facility_ids_filter FROM query_params), 'ARRAY<STRING>'), fr.facility_id))
    ),
    flagged_facilities AS (
      SELECT *,
        CASE WHEN n_equip = 0 THEN (CASE WHEN equipment_missing THEN 'missing_data' ELSE 'true_zero' END) ELSE 'present' END AS equipment_status,
        CASE WHEN n_specialties = 0 THEN (CASE WHEN specialties_missing THEN 'missing_data' ELSE 'true_zero' END) ELSE 'present' END AS specialty_status,
        CASE WHEN n_procedures = 0 THEN (CASE WHEN procedures_missing THEN 'missing_data' ELSE 'true_zero' END) ELSE 'present' END AS procedure_status
      FROM gap_analysis
      WHERE
        (n_equip > 0 OR n_specialties > 0 OR n_procedures > 0)
        AND (n_equip = 0 OR n_specialties = 0 OR n_procedures = 0)
    )
    SELECT to_json(
      map_from_arrays(
        array('query', 'findings', 'data_coverage_summary'),
        array(
          (SELECT parsed_input:query FROM query_params),
          COALESCE(
            to_json(array_agg(named_struct(
              'type',             'facility_profile_counts',
              'facility_id',      facility_id,
              'facility_name',    facility_name,
              'state',            state,
              'city',             city,
              'facility_type',    facility_type,
              'operator_type',    operator_type,
              'equipment_count',  n_equip,
              'equipment_status', equipment_status,
              'specialty_count',  n_specialties,
              'specialty_status', specialty_status,
              'procedure_count',  n_procedures,
              'procedure_status', procedure_status,
              'note', 'If a status is missing_data, the DB lacks records. Only diagnose true gaps if status is true_zero.'
            ))),
            '[]'
          ),
          (SELECT to_json(named_struct(
            'total_facilities',         total_facilities,
            'equipment_coverage_pct',   pct_equipment_known,
            'specialties_coverage_pct', pct_specialties_known,
            'procedures_coverage_pct',  pct_procedures_known,
            'note', 'Before listing any gaps, summarize this coverage data to warn the user about systemic data insufficiency.'
          )) FROM data_context)
        )
      )
    )
    FROM flagged_facilities
  )

  -- ══════════════════════════════════════════════════════════════════════════
  -- Branch 5. Deep Validation (Specialty↔Procedure↔Equipment + Feature Mismatch)
  -- Region OR facility_id OR facility_name is required (guard rail).
  -- All other scope filters applied inline.
  -- ══════════════════════════════════════════════════════════════════════════
  WHEN (SELECT query_lower FROM query_params) RLIKE 'deep valid|validate|consistency|specialty.*match|verify claim|claim.*valid|services.*match|infrastr|capable|feature mismatch|procedure.*equipment|equipment.*procedure|mismatch|equipment count|procedure count'
  THEN
    CASE
      WHEN (SELECT region_filter FROM query_params) IS NULL
       AND (SELECT facility_id_filter FROM query_params) IS NULL
       AND (SELECT facility_name_filter FROM query_params) IS NULL
      THEN to_json(
        map_from_arrays(
          array('query', 'error'),
          array(
            (SELECT parsed_input:query FROM query_params),
            'A region or specific facility is required to scope this analysis. Please specify a region (e.g., "Northern") or a facility name.'
          )
        )
      )
      ELSE (
        WITH
        data_context AS (
          SELECT
            COUNT(*) AS total_in_scope,
            SUM(CASE WHEN (
              (CASE WHEN specialties IS NOT NULL AND size(specialties) > 0 THEN 1 ELSE 0 END) +
              (CASE WHEN procedures  IS NOT NULL AND size(procedures)  > 0 THEN 1 ELSE 0 END) +
              (CASE WHEN equipment   IS NOT NULL AND size(equipment)   > 0 THEN 1 ELSE 0 END)
            ) >= 2 THEN 1 ELSE 0 END) AS validatable,
            SUM(CASE WHEN (
              (CASE WHEN specialties IS NOT NULL AND size(specialties) > 0 THEN 1 ELSE 0 END) +
              (CASE WHEN procedures  IS NOT NULL AND size(procedures)  > 0 THEN 1 ELSE 0 END) +
              (CASE WHEN equipment   IS NOT NULL AND size(equipment)   > 0 THEN 1 ELSE 0 END)
            ) < 2 THEN 1 ELSE 0 END) AS skipped_insufficient_data
          FROM med_atlas_ai.default.facility_records
          WHERE organization_type = 'facility'
            AND ((SELECT facility_id_filter FROM query_params) IS NULL
                 OR facility_id = (SELECT facility_id_filter FROM query_params))
            AND ((SELECT facility_name_filter FROM query_params) IS NULL
                 OR LOWER(facility_name) LIKE '%' || LOWER((SELECT facility_name_filter FROM query_params)) || '%')
            AND ((SELECT region_filter FROM query_params) IS NULL
                 OR state = (SELECT region_filter FROM query_params))
            AND ((SELECT city_filter FROM query_params) IS NULL
                 OR city = (SELECT city_filter FROM query_params))
            AND ((SELECT operator_type_filter FROM query_params) IS NULL
                 OR LOWER(operator_type) = LOWER((SELECT operator_type_filter FROM query_params)))
            AND ((SELECT organization_type_filter FROM query_params) IS NULL
                 OR LOWER(organization_type) = LOWER((SELECT organization_type_filter FROM query_params)))
            AND ((SELECT facility_type_filter FROM query_params) IS NULL
                 OR LOWER(facility_type) = LOWER((SELECT facility_type_filter FROM query_params)))
            AND ((SELECT affiliation_type_filter FROM query_params) IS NULL
                 OR ARRAY_CONTAINS(affiliation_types, (SELECT affiliation_type_filter FROM query_params)))
            AND ((SELECT facility_ids_filter FROM query_params) IS NULL
                 OR ARRAY_CONTAINS(from_json((SELECT facility_ids_filter FROM query_params), 'ARRAY<STRING>'), facility_id))
        ),
        profiles AS (
          SELECT
            fr.facility_id,
            fr.facility_name,
            COALESCE(fr.facility_type, 'unknown')          AS facility_type,
            fr.latitude,
            fr.longitude,
            ARRAY_JOIN(COALESCE(fr.specialties, ARRAY()), ', ') AS specialties_str,
            ARRAY_JOIN(COALESCE(fr.procedures,  ARRAY()), ', ') AS procedures_str,
            ARRAY_JOIN(COALESCE(fr.equipment,   ARRAY()), ', ') AS equipment_str,
            CAST(fr.capacity   AS STRING) AS capacity,
            CAST(fr.no_doctors AS STRING) AS no_doctors,
            CASE
              WHEN fr.specialties IS NOT NULL AND size(fr.specialties) > 0
                AND fr.procedures  IS NOT NULL AND size(fr.procedures)  > 0
                AND fr.equipment   IS NOT NULL AND size(fr.equipment)   > 0
              THEN 'full'
              WHEN fr.equipment IS NULL OR size(COALESCE(fr.equipment, ARRAY())) = 0
              THEN 'partial_no_equipment'
              WHEN fr.procedures IS NULL OR size(COALESCE(fr.procedures, ARRAY())) = 0
              THEN 'partial_no_procedures'
              WHEN fr.specialties IS NULL OR size(COALESCE(fr.specialties, ARRAY())) = 0
              THEN 'partial_no_specialties'
              ELSE 'full'
            END AS completeness
          FROM med_atlas_ai.default.facility_records fr
          WHERE fr.organization_type = 'facility'
            AND ((SELECT facility_id_filter FROM query_params) IS NULL
                 OR fr.facility_id = (SELECT facility_id_filter FROM query_params))
            AND ((SELECT facility_name_filter FROM query_params) IS NULL
                 OR LOWER(fr.facility_name) LIKE '%' || LOWER((SELECT facility_name_filter FROM query_params)) || '%')
            AND ((SELECT region_filter FROM query_params) IS NULL
                 OR fr.state = (SELECT region_filter FROM query_params))
            AND ((SELECT city_filter FROM query_params) IS NULL
                 OR fr.city = (SELECT city_filter FROM query_params))
            AND ((SELECT operator_type_filter FROM query_params) IS NULL
                 OR LOWER(fr.operator_type) = LOWER((SELECT operator_type_filter FROM query_params)))
            AND ((SELECT organization_type_filter FROM query_params) IS NULL
                 OR LOWER(fr.organization_type) = LOWER((SELECT organization_type_filter FROM query_params)))
            AND ((SELECT facility_type_filter FROM query_params) IS NULL
                 OR LOWER(fr.facility_type) = LOWER((SELECT facility_type_filter FROM query_params)))
            AND ((SELECT affiliation_type_filter FROM query_params) IS NULL
                 OR ARRAY_CONTAINS(fr.affiliation_types, (SELECT affiliation_type_filter FROM query_params)))
            AND ((SELECT facility_ids_filter FROM query_params) IS NULL
                 OR ARRAY_CONTAINS(from_json((SELECT facility_ids_filter FROM query_params), 'ARRAY<STRING>'), fr.facility_id))
            AND (
              (CASE WHEN fr.specialties IS NOT NULL AND size(fr.specialties) > 0 THEN 1 ELSE 0 END) +
              (CASE WHEN fr.procedures  IS NOT NULL AND size(fr.procedures)  > 0 THEN 1 ELSE 0 END) +
              (CASE WHEN fr.equipment   IS NOT NULL AND size(fr.equipment)   > 0 THEN 1 ELSE 0 END)
            ) >= 2
        )
        SELECT to_json(
          map_from_arrays(
            array('query', 'findings', 'data_coverage_summary'),
            array(
              (SELECT parsed_input:query FROM query_params),
              COALESCE(
                to_json(array_agg(named_struct(
                  'type',          'deep_validation',
                  'facility_id',   facility_id,
                  'facility_name', facility_name,
                  'facility_type', facility_type,
                  'specialties',   specialties_str,
                  'procedures',    procedures_str,
                  'equipment',     equipment_str,
                  'capacity',      capacity,
                  'no_doctors',    no_doctors,
                  'completeness',  completeness
                ))),
                '[]'
              ),
              (SELECT to_json(named_struct(
                'region_or_facility_scoped', COALESCE(
                  (SELECT facility_name_filter FROM query_params),
                  (SELECT facility_id_filter FROM query_params),
                  (SELECT region_filter FROM query_params)
                ),
                'total_facilities_in_scope', total_in_scope,
                'validatable_facilities',    validatable,
                'skipped_insufficient_data', skipped_insufficient_data,
                'note', 'Start by stating how many facilities were skipped due to insufficient data, then present results.'
              )) FROM data_context)
            )
          )
        )
        FROM profiles
      )
    END

  -- Fallback
  ELSE to_json(
    map_from_arrays(
      array('query', 'findings'),
      array(
        (SELECT parsed_input:query FROM query_params),
        to_json(array(named_struct(
          'type',    'general',
          'message', 'Query recognized but no specific analysis triggered.',
          'hint',    'Supported branches: unmet needs, outlier anomalies, NGO overlap, problem type gaps, deep validation. For contradictions use vector_search_tool. For statistics use genie_chat_tool.'
        )))
      )
    )
  )

  END
  FROM query_params
);