-- =========================================================================
-- Medical Agent UC Function
-- med_atlas_ai.default.analyze_medical_query
-- =========================================================================
-- Pure SQL — works on all Databricks warehouse types.
--
-- Schema (from IDP/storage/models.py):
--   facility_records: facility_id, facility_name, organization_type ('facility'|'ngo'),
--     specialties[], procedures[], equipment[], capabilities[],
--     city, state, country, country_code, capacity, no_doctors,
--     operator_type ('public'|'private'), facility_type ('hospital'|'clinic'|'farmacy'|'doctor'|'dentist'),
--     affiliation_types[], websites[], social_links{}, description, ...
--     created_at, updated_at
--   facility_facts: facility_id, fact_text, fact_type, source_text
--     (fact_type IN: summary, capability, specialty, procedure, equipment)
--   regional_insights: country, state, city, insight_category,
--     insight_value, facility_count, total_capacity, total_doctors
--
-- Args:   query_json  — JSON {query: str, facility_id?: str}
-- Returns: JSON string with findings array
-- =========================================================================

CREATE OR REPLACE FUNCTION med_atlas_ai.default.analyze_medical_query(
  query_json STRING
)
RETURNS STRING
LANGUAGE SQL
COMMENT 'Healthcare anomaly detection — pure SQL'
RETURN CASE

-- Branch 1. Unmet Needs / Regional Gaps
-- Specialties: SQL pre-computes missing_specialties via ARRAY_EXCEPT(global, regional).
-- Procedures & equipment: free-text → passed to LLM for semantic gap reasoning.
WHEN LOWER(parse_json(query_json):query) RLIKE 'unmet|gap|need|service gap'
THEN (
  WITH
  -- Step 1: build the global reference set across ALL facilities in the dataset.
  -- This is the "universe" of known specialties — every distinct value ever recorded.
  global_specialties AS (
    SELECT COLLECT_SET(specialty) AS all_known_specialties
    FROM (
      SELECT EXPLODE(
        COALESCE(specialties, ARRAY())
      ) AS specialty
      FROM med_atlas_ai.default.facility_records
      WHERE organization_type = 'facility'
    )
    WHERE specialty IS NOT NULL AND TRIM(specialty) != ''
  ),

  -- Step 2: aggregate per region — collect everything present in each state.
  region_coverage AS (
    SELECT
      fr.state                                                            AS region,
      COUNT(DISTINCT fr.facility_id)                                      AS total_facilities,
      -- Dedup regional specialties so ARRAY_EXCEPT works correctly
      COLLECT_SET(specialty_val)                                          AS region_specialties,
      FLATTEN(COLLECT_LIST(COALESCE(fr.procedures,   ARRAY())))          AS all_procedures,
      FLATTEN(COLLECT_LIST(COALESCE(fr.equipment,    ARRAY())))          AS all_equipment
    FROM med_atlas_ai.default.facility_records fr
    -- Explode specialties so we can COLLECT_SET on individual values
    LATERAL VIEW OUTER EXPLODE(COALESCE(fr.specialties, ARRAY())) AS specialty_val
    WHERE fr.state IS NOT NULL
      AND fr.organization_type = 'facility'
    GROUP BY fr.state
  )

  -- Step 3: compute the gap — specialties known globally but absent from this region.
  SELECT to_json(
    map_from_arrays(
      array('query', 'findings'),
      array(
        parse_json(query_json):query,
        to_json(array_agg(named_struct(
          'type',                   'regional_coverage',
          'region',                 rc.region,
          'total_facilities',       rc.total_facilities,
          -- Confirmed present in region
          'specialties_present',    rc.region_specialties,
          -- Pre-computed gap: SQL ARRAY_EXCEPT guarantees these are definitively absent
          'specialties_missing',    ARRAY_EXCEPT(gs.all_known_specialties, rc.region_specialties),
          -- Free-text: LLM reasons semantically about what should be present but isn't
          'procedures_present',     rc.all_procedures,
          'equipment_present',      rc.all_equipment,
          'note',                   'specialties_missing is a definitive SQL-computed list of specialties that exist elsewhere in the dataset but not in this region — report these as confirmed gaps. For procedures and equipment (free-text fields), apply your medical domain knowledge to identify what a region of this size and facility count would typically need but appears to lack from the lists provided.'
        )))
      )
    )
  )
  FROM region_coverage rc
  CROSS JOIN global_specialties gs
)

-- Branch 2. Duplicate Facilities (exact name match)
WHEN LOWER(parse_json(query_json):query) RLIKE 'duplicate|duplicat'
THEN (
  WITH dup_counts AS (
    SELECT 
      facility_name,
      COUNT(facility_id) AS duplicate_count,
      collect_list(facility_id) AS facility_ids
    FROM med_atlas_ai.default.facility_records
    WHERE organization_type = 'facility'
      AND facility_name IS NOT NULL AND TRIM(facility_name) != ''
    GROUP BY facility_name
    HAVING COUNT(facility_id) > 1
  )
  SELECT to_json(
    map_from_arrays(
      array('query', 'findings'),
      array(
        parse_json(query_json):query,
        to_json(array_agg(named_struct(
          'type', 'duplicate_facility',
          'facility_name', facility_name,
          'duplicate_count', duplicate_count,
          'facility_ids', facility_ids,
          'severity', 'medium'
        )))
      )
    )
  )
  FROM dup_counts
)

-- Branch 3. Anomaly Flagging (capacity/doctor outliers)
-- Handles 80-85% NULL reality: computes stats only on valid data,
-- explicitly counts NULL-missing facilities, and passes coverage context
-- so the LLM can give an honest, non-misleading answer.
WHEN LOWER(parse_json(query_json):query) RLIKE 'outlier|anomal|flag|unusual|inconsisten|signal'
THEN (
  WITH
  -- Step 1: Count valid vs missing data across the full dataset
  data_context AS (
    SELECT
      COUNT(*)                                                       AS total_facilities,
      COUNT(capacity)                                                AS has_capacity,
      COUNT(no_doctors)                                              AS has_doctors,
      total_facilities - has_capacity                                AS missing_capacity,
      total_facilities - has_doctors                                 AS missing_doctors,
      ROUND(100.0 * has_capacity  / NULLIF(total_facilities, 0), 1) AS pct_capacity_known,
      ROUND(100.0 * has_doctors   / NULLIF(total_facilities, 0), 1) AS pct_doctors_known
    FROM med_atlas_ai.default.facility_records
    WHERE organization_type = 'facility'
  ),

  -- Step 2: Compute mean/std ONLY on the non-NULL subset
  -- Stats are valid but come from a small sample — context provided to LLM
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

  -- Step 3: Flag mathematical outliers (3-sigma) on facilities WITH data
  outliers AS (
    -- Capacity outliers
    SELECT
      fr.facility_id,
      fr.facility_name,
      fr.latitude,
      fr.longitude,
      COALESCE(fr.facility_type, 'unknown')                         AS facility_type,
      'beds'                                                         AS measurement,
      CAST(fr.capacity AS INT)                                       AS reported_value,
      ROUND(cs.m_cap, 0)                                             AS typical_value,
      'unusual_value'                                                AS flag_type,
      'This facility reports ' || fr.capacity || ' beds. '
        || 'Most similar facilities in our dataset have around '
        || CAST(ROUND(cs.m_cap, 0) AS STRING)
        || ' beds — this number is significantly outside the normal range and may be an error or an exceptionally large facility.'
                                                                     AS plain_reason
    FROM med_atlas_ai.default.facility_records fr
    CROSS JOIN cap_stats cs
    WHERE fr.organization_type = 'facility'
      AND fr.capacity IS NOT NULL AND CAST(fr.capacity AS INT) > 0
      AND cs.m_cap IS NOT NULL AND cs.s_cap IS NOT NULL
      AND (
        CAST(fr.capacity AS DOUBLE) < cs.m_cap - 3 * cs.s_cap
        OR CAST(fr.capacity AS DOUBLE) > cs.m_cap + 3 * cs.s_cap
      )

    UNION ALL

    -- Doctor count outliers
    SELECT
      fr.facility_id,
      fr.facility_name,
      fr.latitude,
      fr.longitude,
      COALESCE(fr.facility_type, 'unknown')                          AS facility_type,
      'doctors'                                                       AS measurement,
      CAST(fr.no_doctors AS INT)                                      AS reported_value,
      ROUND(cs.m_docs, 0)                                             AS typical_value,
      'unusual_value'                                                 AS flag_type,
      'This facility reports ' || fr.no_doctors || ' doctors. '
        || 'Most similar facilities in our dataset have around '
        || CAST(ROUND(cs.m_docs, 0) AS STRING)
        || ' doctors — this count is significantly outside the normal range and may need verification.'
                                                                      AS plain_reason
    FROM med_atlas_ai.default.facility_records fr
    CROSS JOIN cap_stats cs
    WHERE fr.organization_type = 'facility'
      AND fr.no_doctors IS NOT NULL AND CAST(fr.no_doctors AS INT) > 0
      AND cs.m_docs IS NOT NULL AND cs.s_docs IS NOT NULL
      AND (
        CAST(fr.no_doctors AS DOUBLE) < cs.m_docs - 3 * cs.s_docs
        OR CAST(fr.no_doctors AS DOUBLE) > cs.m_docs + 3 * cs.s_docs
      )
  )

  SELECT to_json(
    map_from_arrays(
      array('query', 'findings', 'data_coverage_summary'),
      array(
        parse_json(query_json):query,

        -- Outlier rows (empty array if none found)
        COALESCE(
          to_json(array_agg(named_struct(
            'type',           'anomaly_flagging',
            'facility_id',    facility_id,
            'facility_name',  facility_name,
            'facility_type',  facility_type,
            'latitude',       latitude,
            'longitude',      longitude,
            'measurement',    measurement,
            'reported_value', reported_value,
            'typical_value',  typical_value,
            'flag_type',      flag_type,
            'reason',         plain_reason
          ))),
          '[]'
        ),

        -- Coverage context: always tell the LLM how incomplete the dataset is
        -- so it can give an honest caveat to the user
        (SELECT to_json(named_struct(
          'total_facilities',    total_facilities,
          'beds_data_known_for', has_capacity,
          'beds_data_missing',   missing_capacity,
          'beds_coverage_pct',   pct_capacity_known,
          'doctors_data_known_for', has_doctors,
          'doctors_data_missing',   missing_doctors,
          'doctors_coverage_pct',   pct_doctors_known,
          'note', 'IMPORTANT: Always start your response by telling the user how many facilities have unknown bed/doctor data. These facilities could not be checked for anomalies. Do NOT skip this — it is critical context. Phrase it simply, e.g. "Please note that bed count data is only available for X% of facilities — the remaining Y facilities could not be assessed."'
        )) FROM data_context)
      )
    )
  )
  FROM (
    SELECT * FROM outliers
    ORDER BY ABS(reported_value - typical_value) DESC
    LIMIT 20
  )
)



-- Branch 4. Feature Mismatch (raw procedure vs equipment counts)
-- Detects medical implausibility or missing data context
WHEN LOWER(parse_json(query_json):query) RLIKE 'feature mismatch|procedure count|equipment count|mismatch'
THEN (
  WITH
  -- Step 1: Count how many facilities with procedures are missing equipment data
  data_context AS (
    SELECT
      COUNT(DISTINCT facility_id) AS total_with_procedures,
      COUNT(DISTINCT CASE WHEN equipment IS NOT NULL THEN facility_id END) AS has_equipment,
      COUNT(DISTINCT CASE WHEN equipment IS NULL THEN facility_id END) AS missing_equipment,
      ROUND(100.0 * COUNT(DISTINCT CASE WHEN equipment IS NOT NULL THEN facility_id END) / NULLIF(COUNT(DISTINCT facility_id), 0), 1) AS pct_equipment_known
    FROM med_atlas_ai.default.facility_records
    WHERE organization_type = 'facility' 
      AND procedures IS NOT NULL 
      AND size(procedures) > 0
  ),

  -- Step 2: Compute ratios and tag missing data vs true zero
  mismatches AS (
    SELECT
      facility_id,
      facility_name,
      latitude,
      longitude,
      COALESCE(facility_type, 'unknown') AS facility_type,
      size(procedures) AS n_procedures,
      COALESCE(size(equipment), 0) AS n_equipment,
      -- Only calculate ratio if equipment data actually exists (even if []). If NULL, ratio is NULL.
      CASE WHEN equipment IS NOT NULL
           THEN ROUND(CAST(size(procedures) AS DOUBLE) / GREATEST(COALESCE(size(equipment), 0), 1), 2)
           ELSE NULL
      END AS ratio,
      -- Explicitly tag the difference between "zero equipment found" and "equipment field is NULL"
      CASE WHEN equipment IS NULL THEN 'missing_equipment'
           ELSE 'implausible_ratio'
      END AS flag_type
    FROM med_atlas_ai.default.facility_records
    WHERE organization_type = 'facility'
      AND procedures IS NOT NULL 
      AND size(procedures) > 0
      -- Implausible Threshold: Only pass to LLM if missing, OR if the ratio > 5.0
      AND (
        equipment IS NULL 
        OR (CAST(size(procedures) AS DOUBLE) / GREATEST(COALESCE(size(equipment), 0), 1)) > 5.0
      )
  )

  SELECT to_json(
    map_from_arrays(
      array('query', 'findings', 'data_coverage_summary'),
      array(
        parse_json(query_json):query,

        COALESCE(
          to_json(array_agg(named_struct(
            'type',           'feature_mismatch_raw',
            'facility_id',    facility_id,
            'facility_name',  facility_name,
            'facility_type',  facility_type,
            'latitude',       latitude,
            'longitude',      longitude,
            'n_procedures',   n_procedures,
            'n_equipment',    n_equipment,
            'ratio',          ratio,
            'flag_type',      flag_type,
            'note',           CASE WHEN flag_type = 'missing_equipment'
                                   THEN 'Unverifiable: This facility claims ' || n_procedures || ' procedures, but no equipment records exist in the database to verify how they perform them.'
                                   ELSE 'Evaluate if the procedure-to-equipment ratio of ' || CAST(ratio AS STRING) || ' is medically implausible for a ' || facility_type || '.'
                              END
          ))),
          '[]'
        ),

        (SELECT to_json(named_struct(
          'facilities_with_procedures', total_with_procedures,
          'equipment_data_known_for',   has_equipment,
          'unprocessed_due_to_nulls',   missing_equipment,
          'equipment_coverage_pct',     pct_equipment_known,
          'note', 'Start by stating that ' || missing_equipment || ' facilities could not be fully analyzed because their equipment data is NULL. Then present the top implausible ratios.'
        )) FROM data_context)
      )
    )
  )
  FROM (
    SELECT * FROM mismatches
    ORDER BY ratio DESC NULLS LAST
    LIMIT 35
  )
)



-- Branch 5. NGO Overlap (grouped by affiliation_type + region)
-- Returns raw NGO groupings. LLM identifies true overlaps using world knowledge.
WHEN LOWER(parse_json(query_json):query) RLIKE 'ngo overlap|overlapping ngo|same ngo|same region'
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
    GROUP BY fr.state, fr.city, ARRAY_JOIN(COALESCE(fr.affiliation_types, ARRAY()), ',')
    HAVING COUNT(DISTINCT fr.facility_id) > 1
  )
  SELECT to_json(
    map_from_arrays(
      array('query', 'findings'),
      array(
        parse_json(query_json):query,
        to_json(array_agg(named_struct(
          'type', 'ngo_overlap_raw',
          'region', region,
          'city', city,
          'affiliation_key', affiliation_key,
          'n_facilities', n_facilities,
          'facilities', facility_list,
          'note', 'Determine if these represent duplicated efforts or complementary services'
        )))
      )
    )
  )
  FROM ngo_by_affiliation
)

-- Branch 6. Problem Type Classification (raw counts — LLM classifies gap type)
-- Returns per-facility counts. The LLM classifies gap type based on the
-- facility_type enum (hospital/clinic/dentist/farmacy) and the counts.
WHEN LOWER(parse_json(query_json):query) RLIKE 'problem type|root cause|gap type|classify gap|workforce|staffing|equipment gap|staff shortage'
THEN (
  WITH
  -- Step 1: establish dataset completeness
  data_context AS (
    SELECT
      COUNT(facility_id) AS total_facilities,
      COUNT(equipment) AS has_equipment,
      COUNT(specialties) AS has_specialties,
      COUNT(procedures) AS has_procedures,
      ROUND(100.0 * COUNT(equipment) / NULLIF(COUNT(facility_id), 0), 1) AS pct_equipment_known,
      ROUND(100.0 * COUNT(specialties) / NULLIF(COUNT(facility_id), 0), 1) AS pct_specialties_known,
      ROUND(100.0 * COUNT(procedures) / NULLIF(COUNT(facility_id), 0), 1) AS pct_procedures_known
    FROM med_atlas_ai.default.facility_records
    WHERE organization_type = 'facility'
  ),

  -- Step 2: count facts per facility and check if the field itself is NULL
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
      COALESCE(CAST(fr.capacity AS INT), 0) AS capacity,
      COALESCE(CAST(fr.no_doctors AS INT), 0) AS no_doctors,

      (SELECT COUNT(*) FROM med_atlas_ai.default.facility_facts ff WHERE ff.facility_id = fr.facility_id AND ff.fact_type = 'equipment') AS n_equip,
      fr.equipment IS NULL AS equipment_missing,

      (SELECT COUNT(*) FROM med_atlas_ai.default.facility_facts ff WHERE ff.facility_id = fr.facility_id AND ff.fact_type = 'specialty') AS n_specialties,
      fr.specialties IS NULL AS specialties_missing,

      (SELECT COUNT(*) FROM med_atlas_ai.default.facility_facts ff WHERE ff.facility_id = fr.facility_id AND ff.fact_type = 'procedure') AS n_procedures,
      fr.procedures IS NULL AS procedures_missing
    FROM med_atlas_ai.default.facility_records fr
    WHERE organization_type = 'facility'
  ),

  -- Step 3: filter to facilities that are partially missing something,
  -- but strictly exclude facilities that are 100% empty (to save LLM context window)
  flagged_facilities AS (
    SELECT *,
      CASE WHEN n_equip = 0 THEN (CASE WHEN equipment_missing THEN 'missing_data' ELSE 'true_zero' END) ELSE 'present' END AS equipment_status,
      CASE WHEN n_specialties = 0 THEN (CASE WHEN specialties_missing THEN 'missing_data' ELSE 'true_zero' END) ELSE 'present' END AS specialty_status,
      CASE WHEN n_procedures = 0 THEN (CASE WHEN procedures_missing THEN 'missing_data' ELSE 'true_zero' END) ELSE 'present' END AS procedure_status
    FROM gap_analysis
    WHERE
      -- Must have at least SOME medical data (so it's a functioning, scraped facility)
      (n_equip > 0 OR n_specialties > 0 OR n_procedures > 0)
      AND
      -- And it must be missing/zero in at least one OTHER category
      (n_equip = 0 OR n_specialties = 0 OR n_procedures = 0)
  )

  SELECT to_json(
    map_from_arrays(
      array('query', 'findings', 'data_coverage_summary'),
      array(
        parse_json(query_json):query,

        COALESCE(
          to_json(array_agg(named_struct(
            'type', 'facility_profile_counts',
            'facility_id', facility_id,
            'facility_name', facility_name,
            'state', state,
            'city', city,
            'latitude', latitude,
            'longitude', longitude,
            'facility_type', facility_type,
            'operator_type', operator_type,
            'equipment_count', n_equip,
            'equipment_status', equipment_status,
            'specialty_count', n_specialties,
            'specialty_status', specialty_status,
            'procedure_count', n_procedures,
            'procedure_status', procedure_status,
            'note', 'If a status is missing_data, the database simply lacks records for it. Only diagnose true medical gaps (e.g. service_gap, equipment_gap) if the status is true_zero.'
          ))),
          '[]'
        ),

        (SELECT to_json(named_struct(
          'total_facilities', total_facilities,
          'equipment_coverage_pct', pct_equipment_known,
          'specialties_coverage_pct', pct_specialties_known,
          'procedures_coverage_pct', pct_procedures_known,
          'note', 'Before listing any gaps, summarize this coverage data to warn the user about systemic data insufficiency.'
        )) FROM data_context)
      )
    )
  )
  FROM flagged_facilities
)

-- Fallback
ELSE to_json(
  map_from_arrays(
    array('query', 'findings'),
    array(
      parse_json(query_json):query,
      to_json(array(named_struct(
        'type', 'general',
        'message', 'Query recognized but no specific analysis triggered.',
        'hint', 'Supported (6 branches): NGO classification/overlap, unmet needs/regional gaps, duplicate detection, outlier/anomaly flagging, feature mismatch, problem type/gap classification. For CONTRADICTIONS use vector_search_tool. For oversupply/scarcity/specialist distribution/web presence use genie_chat_tool.'
      )))
    )
  )
)

END;