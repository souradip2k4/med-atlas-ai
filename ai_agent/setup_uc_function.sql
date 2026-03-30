-- =========================================================================
-- Medical Agent UC Function
-- med_atlas_ai.default.analyze_medical_query
-- =========================================================================
-- Pure SQL — works on all Databricks warehouse types.
--
-- Schema (from IDP/storage/models.py):
--   facility_records: facility_id, facility_name, organization_type ('facility'|'ngo'),
--     specialties[], procedures[], equipment[], capabilities[],
--     city, state, country, country_code, no_beds, no_doctors,
--     operator_type ('public'|'private'), facility_type ('hospital'|'clinic'|'farmacy'|'doctor'|'dentist'),
--     affiliation_types[], websites[], social_links{}, description, ...
--     created_at, updated_at
--   facility_facts: facility_id, fact_text, fact_type, source_text
--     (fact_type IN: summary, capability, specialty, procedure, equipment)
--   regional_insights: country, state, city, insight_category,
--     insight_value, facility_count, total_beds, total_doctors
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

-- 1. Contradictory Signals (ICU level conflicts across fact rows for same facility)
WHEN LOWER(parse_json(query_json):query) RLIKE 'anomal|inconsisten|contradict|signal'
THEN (
  WITH icu_facts AS (
    SELECT facility_id, collect_list(fact_text) AS statements
    FROM med_atlas_ai.default.facility_facts
    WHERE LOWER(fact_text) RLIKE 'icu|intensive care|critical care'
    GROUP BY facility_id
    HAVING COUNT(*) > 1
  )
  SELECT to_json(
    map_from_arrays(
      array('query', 'findings'),
      array(
        parse_json(query_json):query,
        to_json(array_agg(
          named_struct(
            'type', 'contradictory_signals',
            'facility_id', facility_id,
            'conflicting_statements', statements,
            'severity', 'high'
          )
        ))
      )
    )
  )
  FROM icu_facts
)

-- 2. NGO Data (classification delegated to LLM)
-- Returns raw NGO data. The LLM classifies direct_operator / supporter / none
-- based on facility_name and affiliation_types, using world knowledge.
WHEN LOWER(parse_json(query_json):query) RLIKE 'ngo|classification|classify'
THEN (
  SELECT to_json(
    map_from_arrays(
      array('query', 'findings'),
      array(
        parse_json(query_json):query,
        to_json(array_agg(named_struct(
          'type', 'ngo_raw_data',
          'facility_id', facility_id,
          'facility_name', facility_name,
          'operator_type', COALESCE(operator_type, 'unknown'),
          'affiliation_types', affiliation_types,
          'note', 'Classify as direct_operator/supporter/none using facility_name and affiliation_types'
        )))
      )
    )
  )
  FROM med_atlas_ai.default.facility_records
  WHERE organization_type = 'ngo'
)

-- 3. Reliability Scoring
WHEN LOWER(parse_json(query_json):query) RLIKE 'reliab|score|quality'
THEN (
  WITH scored AS (
    SELECT
      fr.facility_id,
      fr.facility_name,
      COALESCE(fr.facility_type, 'unknown') AS facility_type,
      COALESCE(ff.cnt, 0) AS n_facts,
      COALESCE(CAST(fr.no_beds AS INT), 0) AS no_beds,
      COALESCE(fr.no_doctors, 0) AS no_doctors,
      70
        - CASE WHEN COALESCE(ff.cnt, 0) < 2 THEN 20
               WHEN COALESCE(ff.cnt, 0) < 4 THEN 10
               ELSE 0 END
        - CASE WHEN fr.no_beds IS NOT NULL AND CAST(fr.no_beds AS INT) > 500 THEN 15 ELSE 0 END
        - CASE WHEN fr.no_beds IS NULL AND fr.facility_type = 'hospital' THEN 10 ELSE 0 END
        AS reliability_score
    FROM med_atlas_ai.default.facility_records fr
    LEFT JOIN (
      SELECT facility_id, COUNT(*) AS cnt
      FROM med_atlas_ai.default.facility_facts
      GROUP BY facility_id
    ) ff ON fr.facility_id = ff.facility_id
    WHERE fr.organization_type = 'facility'
  )
  SELECT to_json(
    map_from_arrays(
      array('query', 'findings'),
      array(
        parse_json(query_json):query,
        to_json(array_agg(named_struct(
          'type', 'reliability_score',
          'facility_id', facility_id,
          'facility_name', facility_name,
          'facility_type', facility_type,
          'reliability_score', reliability_score,
          'rating',
            CASE WHEN reliability_score >= 80 THEN 'high'
                 WHEN reliability_score >= 60 THEN 'medium'
                 ELSE 'low' END,
          'n_facts', n_facts,
          'no_beds', no_beds,
          'no_doctors', no_doctors
        )))
      )
    )
  )
  FROM scored
  WHERE reliability_score < 75
)

-- 4. Unmet Needs / Regional Gaps
-- Returns full (state, specialties, procedures) coverage per region.
-- LLM determines which services are missing based on medical domain knowledge.
WHEN LOWER(parse_json(query_json):query) RLIKE 'unmet|gap|need|service gap'
THEN (
  WITH region_coverage AS (
    SELECT
      fr.state AS region,
      COUNT(DISTINCT fr.facility_id) AS total_facilities,
      FLATTEN(COLLECT_LIST(COALESCE(fr.specialties, ARRAY()))) AS all_specialties,
      FLATTEN(COLLECT_LIST(COALESCE(fr.procedures, ARRAY()))) AS all_procedures,
      FLATTEN(COLLECT_LIST(COALESCE(fr.equipment, ARRAY()))) AS all_equipment
    FROM med_atlas_ai.default.facility_records fr
    WHERE fr.state IS NOT NULL
      AND fr.organization_type = 'facility'
    GROUP BY fr.state
  )
  SELECT to_json(
    map_from_arrays(
      array('query', 'findings'),
      array(
        parse_json(query_json):query,
        to_json(array_agg(named_struct(
          'type', 'regional_coverage',
          'region', region,
          'total_facilities', total_facilities,
          'specialties_present', all_specialties,
          'procedures_present', all_procedures,
          'equipment_present', all_equipment,
          'note', 'Identify missing critical services using medical domain knowledge'
        )))
      )
    )
  )
  FROM region_coverage
)

-- 5. Duplicate Facilities (exact name match)
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

-- 6. Anomaly Flagging (bed count outliers)
WHEN LOWER(parse_json(query_json):query) RLIKE 'outlier|anomal|flag|unusual'
THEN (
  WITH cap_stats AS (
    SELECT
      AVG(CAST(no_beds AS DOUBLE)) AS m_beds,
      STDDEV(CAST(no_beds AS DOUBLE)) AS s_beds,
      AVG(CAST(no_doctors AS DOUBLE)) AS m_docs,
      STDDEV(CAST(no_doctors AS DOUBLE)) AS s_docs
    FROM med_atlas_ai.default.facility_records
    WHERE organization_type = 'facility'
  ),
  outliers AS (
    SELECT
      fr.facility_id,
      fr.facility_name,
      COALESCE(fr.facility_type, 'unknown') AS facility_type,
      CAST(fr.no_beds AS INT) AS value,
      ROUND(cs.m_beds, 1) AS mean,
      ROUND(cs.s_beds, 1) AS std,
      'no_beds' AS field,
      'high' AS severity,
      'Outlier: ' || fr.no_beds || ' beds (mean=' || ROUND(cs.m_beds, 0) || ')' AS reason
    FROM med_atlas_ai.default.facility_records fr, cap_stats cs
    WHERE fr.organization_type = 'facility'
      AND fr.no_beds IS NOT NULL AND CAST(fr.no_beds AS INT) > 0
      AND cs.m_beds IS NOT NULL AND cs.s_beds IS NOT NULL
      AND (
        CAST(fr.no_beds AS DOUBLE) < cs.m_beds - 3 * cs.s_beds
        OR CAST(fr.no_beds AS DOUBLE) > cs.m_beds + 3 * cs.s_beds
      )
    UNION ALL
    SELECT
      fr.facility_id,
      fr.facility_name,
      COALESCE(fr.facility_type, 'unknown') AS facility_type,
      CAST(fr.no_doctors AS INT) AS value,
      ROUND(cs.m_docs, 1) AS mean,
      ROUND(cs.s_docs, 1) AS std,
      'no_doctors' AS field,
      'high' AS severity,
      'Outlier: ' || fr.no_doctors || ' doctors (mean=' || ROUND(cs.m_docs, 0) || ')' AS reason
    FROM med_atlas_ai.default.facility_records fr, cap_stats cs
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
      array('query', 'findings'),
      array(
        parse_json(query_json):query,
        CASE WHEN (SELECT COUNT(*) FROM outliers) = 0
             THEN '[]'
             ELSE to_json(array_agg(named_struct(
               'type', 'anomaly_flagging',
               'field', field,
               'facility_id', facility_id,
               'facility_name', facility_name,
               'facility_type', facility_type,
               'value', value,
               'mean', mean,
               'std', std,
               'severity', severity,
               'reason', reason
             )))
        END
      )
    )
  )
  FROM outliers
)

-- 7. Feature Mismatch (raw procedure vs equipment counts — LLM classifies)
-- Returns counts. LLM determines if the ratio of procedures to equipment
-- is medically implausible based on the specific facility type.
WHEN LOWER(parse_json(query_json):query) RLIKE 'feature mismatch|procedure count|equipment count|mismatch'
THEN (
  WITH proc_count AS (
    SELECT facility_id,
      COUNT(*) AS n_procedures
    FROM med_atlas_ai.default.facility_facts
    WHERE fact_type = 'procedure'
    GROUP BY facility_id
  ),
  equip_count AS (
    SELECT facility_id,
      COUNT(*) AS n_equipment
    FROM med_atlas_ai.default.facility_facts
    WHERE fact_type = 'equipment'
    GROUP BY facility_id
  )
  SELECT to_json(
    map_from_arrays(
      array('query', 'findings'),
      array(
        parse_json(query_json):query,
        to_json(array_agg(named_struct(
          'type', 'feature_mismatch_raw',
          'facility_id', p.facility_id,
          'facility_name', fr.facility_name,
          'facility_type', COALESCE(fr.facility_type, 'unknown'),
          'n_procedures', p.n_procedures,
          'n_equipment', COALESCE(e.n_equipment, 0),
          'ratio', ROUND(CAST(p.n_procedures AS DOUBLE) / GREATEST(e.n_equipment, 1), 2),
          'note', 'Evaluate if the procedure-to-equipment ratio is medically implausible for this facility_type'
        )))
      )
    )
  )
  FROM proc_count p
  JOIN med_atlas_ai.default.facility_records fr ON p.facility_id = fr.facility_id
  LEFT JOIN equip_count e ON p.facility_id = e.facility_id
  WHERE p.n_procedures > 0
)

-- 8. NGO Overlap (grouped by affiliation_type + region)
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

-- 9. Problem Type Classification (raw counts — LLM classifies gap type)
-- Returns per-facility counts only. The LLM classifies gap type based on the
-- facility_type enum (hospital/clinic/dentist/farmacy) and the counts.
WHEN LOWER(parse_json(query_json):query) RLIKE 'problem type|root cause|gap type|classify gap|workforce|staffing|equipment gap|staff shortage'
THEN (
  WITH gap_analysis AS (
    SELECT
      fr.facility_id,
      fr.facility_name,
      fr.state,
      fr.city,
      COALESCE(fr.facility_type, 'unknown') AS facility_type,
      fr.operator_type,
      COALESCE(CAST(fr.no_beds AS INT), 0) AS no_beds,
      COALESCE(CAST(fr.no_doctors AS INT), 0) AS no_doctors,
      (SELECT COUNT(*) FROM med_atlas_ai.default.facility_facts ff
       WHERE ff.facility_id = fr.facility_id AND ff.fact_type = 'equipment') AS n_equip,
      (SELECT COUNT(*) FROM med_atlas_ai.default.facility_facts ff
       WHERE ff.facility_id = fr.facility_id AND ff.fact_type = 'specialty') AS n_specialties,
      (SELECT COUNT(*) FROM med_atlas_ai.default.facility_facts ff
       WHERE ff.facility_id = fr.facility_id AND ff.fact_type = 'procedure') AS n_procedures
    FROM med_atlas_ai.default.facility_records fr
    WHERE organization_type = 'facility'
  )
  SELECT to_json(
    map_from_arrays(
      array('query', 'findings'),
      array(
        parse_json(query_json):query,
        to_json(array_agg(named_struct(
          'type', 'facility_profile_counts',
          'facility_id', facility_id,
          'facility_name', facility_name,
          'state', state,
          'city', city,
          'facility_type', facility_type,
          'operator_type', operator_type,
          'no_beds', no_beds,
          'no_doctors', no_doctors,
          'equipment_count', n_equip,
          'specialty_count', n_specialties,
          'procedure_count', n_procedures,
          'note', 'Classify gap type (equipment_gap/service_gap/overclaim_gap/balanced) using medical domain knowledge'
        )))
      )
    )
  )
  FROM gap_analysis
  WHERE n_equip = 0 OR n_specialties = 0 OR n_procedures = 0
)

-- 10. Data Staleness (updated_at age scoring)
WHEN LOWER(parse_json(query_json):query) RLIKE 'staleness|stale|data age|data outdated|outdated|when updated|last updated'
THEN (
  WITH staleness AS (
    SELECT
      fr.facility_id,
      fr.facility_name,
      fr.updated_at,
      DATEDIFF(CURRENT_TIMESTAMP(), fr.updated_at) AS days_old,
      DATEDIFF(CURRENT_TIMESTAMP(), fr.created_at) AS days_since_created,
      CASE
        WHEN fr.updated_at IS NULL THEN 'unknown'
        WHEN DATEDIFF(CURRENT_TIMESTAMP(), fr.updated_at) IS NULL THEN 'unknown'
        WHEN DATEDIFF(CURRENT_TIMESTAMP(), fr.updated_at) > 365 THEN 'stale'
        WHEN DATEDIFF(CURRENT_TIMESTAMP(), fr.updated_at) > 180 THEN 'aging'
        WHEN DATEDIFF(CURRENT_TIMESTAMP(), fr.updated_at) > 90 THEN 'moderate'
        ELSE 'current'
      END AS data_status
    FROM med_atlas_ai.default.facility_records fr
    WHERE organization_type = 'facility'
  )
  SELECT to_json(
    map_from_arrays(
      array('query', 'findings'),
      array(
        parse_json(query_json):query,
        to_json(array_agg(named_struct(
          'type', 'data_staleness',
          'facility_id', facility_id,
          'facility_name', facility_name,
          'days_since_update', days_old,
          'days_since_created', days_since_created,
          'data_status', data_status,
          'updated_at', CAST(updated_at AS STRING),
          'severity',
            CASE data_status
              WHEN 'stale' THEN 'high'
              WHEN 'aging' THEN 'medium'
              WHEN 'moderate' THEN 'low'
              ELSE 'info'
            END,
          'recommendation',
            CASE data_status
              WHEN 'stale' THEN 'Data is over 1 year old — schedule re-verification'
              WHEN 'aging' THEN 'Data is 6-12 months old — verify with facility'
              WHEN 'moderate' THEN 'Data is 3-6 months old — update soon'
              ELSE 'Data is current'
            END
        )))
      )
    )
  )
  FROM staleness
  WHERE data_status IN ('stale', 'aging', 'moderate')
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
        'hint', 'Supported: anomalies/contradictions, reliability score, NGO classification, unmet needs/regional gaps, duplicates, outlier flagging, feature mismatch, NGO overlap, problem type/gap classification, data staleness. For oversupply/scarcity, specialist distribution, or web presence queries, use genie_chat_tool instead.'
      )))
    )
  )
)

END;