-- =========================================================================
-- Medical Agent UC Function
-- med_atlas_ai.default.analyze_medical_query
-- =========================================================================
-- Pure SQL — works on all Databricks warehouse types.
--
-- Schema (from IDP/storage/models.py):
--   facility_records: facility_id, facility_name, organization_type,
--     specialties[], procedures[], equipment[], capabilities[],
--     city, state, country, number_doctors, capacity,
--     operator_type, facility_type, affiliation_types[], ...
--   facility_facts: facility_id, fact_text, fact_type, source_text
--     (fact_type IN: summary, capability, specialty, procedure, equipment)
--   regional_insights: country, state, city, insight_category,
--     insight_value, facility_count, total_beds, total_doctors, ...
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

-- 2. NGO Classification
WHEN LOWER(parse_json(query_json):query) RLIKE 'ngo|classification|classify'
THEN (
  SELECT to_json(
    map_from_arrays(
      array('query', 'findings'),
      array(
        parse_json(query_json):query,
        to_json(array_agg(named_struct(
          'type', 'ngo_classification',
          'facility_id', facility_id,
          'facility_name', facility_name,
          'ngo_level',
            CASE
              WHEN LOWER(facility_name) RLIKE 'ngo|mission|charity|catholic|methodist|islamic|red cross|unicef|who|foundation|church'
                THEN 'direct_operator'
              WHEN affil_str != '' AND affil_str IS NOT NULL
                AND affil_str RLIKE '%ngo%|%mission%|%charity%|%church%|%foundation%'
                THEN 'supporter'
              ELSE 'none'
            END,
          'operator_type', COALESCE(operator_type, 'unknown'),
          'severity', 'low'
        )))
      )
    )
  )
  FROM (
    SELECT
      facility_id, facility_name, operator_type,
      ARRAY_JOIN(COALESCE(affiliation_types, ARRAY()), ',') AS affil_str
    FROM med_atlas_ai.default.facility_records
  )
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
      COALESCE(CAST(fr.capacity AS INT), 0) AS capacity,
      COALESCE(fr.number_doctors, 0) AS number_doctors,
      70
        - CASE WHEN COALESCE(ff.cnt, 0) < 2 THEN 20
               WHEN COALESCE(ff.cnt, 0) < 4 THEN 10
               ELSE 0 END
        - CASE WHEN fr.capacity IS NOT NULL AND CAST(fr.capacity AS INT) > 500 THEN 15 ELSE 0 END
        - CASE WHEN fr.capacity IS NULL AND fr.facility_type = 'hospital' THEN 10 ELSE 0 END
        AS reliability_score
    FROM med_atlas_ai.default.facility_records fr
    LEFT JOIN (
      SELECT facility_id, COUNT(*) AS cnt
      FROM med_atlas_ai.default.facility_facts
      GROUP BY facility_id
    ) ff ON fr.facility_id = ff.facility_id
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
          'capacity', capacity,
          'number_doctors', number_doctors
        )))
      )
    )
  )
  FROM scored
  WHERE reliability_score < 75
)

-- 4. Over-claiming (clinic/dentist claiming hospital-level services)
WHEN LOWER(parse_json(query_json):query) RLIKE 'over-claim|implausib|service claim'
THEN (
  SELECT to_json(
    map_from_arrays(
      array('query', 'findings'),
      array(
        parse_json(query_json):query,
        to_json(array_agg(named_struct(
          'type', 'over_claiming',
          'facility_id', facility_id,
          'facility_name', facility_name,
          'facility_type', ftype,
          'implausible_service', svc_text,
          'severity', 'high',
          'reason', 'A ' || ftype || ' should not provide emergency/ICU/inpatient services'
        )))
      )
    )
  )
  FROM (
    SELECT DISTINCT
      fr.facility_id,
      fr.facility_name,
      COALESCE(fr.facility_type, 'unknown') AS ftype,
      LOWER(ff.fact_text) AS svc_text
    FROM med_atlas_ai.default.facility_records fr
    JOIN med_atlas_ai.default.facility_facts ff ON fr.facility_id = ff.facility_id
    WHERE LOWER(COALESCE(fr.facility_type, '')) IN ('clinic', 'dentist', 'pharmacy')
      AND (
        LOWER(ff.fact_text) RLIKE 'emergency room|emergency department|icu|intensive care|open heart|organ transplant|brain surgery'
        OR LOWER(ff.fact_text) RLIKE 'inpatient|24-hour emergency'
      )
  )
)

-- 5. Equipment–Procedure Mismatch
WHEN LOWER(parse_json(query_json):query) RLIKE 'equipment|mismatch'
THEN (
  WITH equip AS (
    SELECT
      e.facility_id,
      fr.facility_name,
      LOWER(e.fact_text) AS equip_lower,
      e.fact_text AS equipment_fact
    FROM med_atlas_ai.default.facility_facts e
    JOIN med_atlas_ai.default.facility_records fr ON e.facility_id = fr.facility_id
    WHERE e.fact_type = 'equipment'
  ),
  caps AS (
    SELECT
      facility_id,
      LOWER(ARRAY_JOIN(collect_list(fact_text), ' ')) AS cap_text
    FROM med_atlas_ai.default.facility_facts
    WHERE fact_type IN ('capability', 'procedure')
    GROUP BY facility_id
  )
  SELECT to_json(
    map_from_arrays(
      array('query', 'findings'),
      array(
        parse_json(query_json):query,
        to_json(array_agg(named_struct(
          'type', 'equipment_procedure_mismatch',
          'facility_id', eq.facility_id,
          'facility_name', eq.facility_name,
          'equipment', eq.equipment_fact,
          'issue',
            CASE
              WHEN eq.equip_lower RLIKE 'mri|magnetic resonance'
                AND (cp.cap_text IS NULL OR NOT cp.cap_text RLIKE 'radiology|radiographer|mri')
                THEN 'MRI without radiology/radiographer support'
              WHEN eq.equip_lower RLIKE 'ct scan|computed tomography'
                AND (cp.cap_text IS NULL OR NOT cp.cap_text RLIKE 'radiology|radiographer|ct')
                THEN 'CT scan without radiology/radiographer support'
              WHEN eq.equip_lower RLIKE 'dialysis|kidney'
                AND (cp.cap_text IS NULL OR NOT cp.cap_text RLIKE 'nephrology|dialysis|kidney')
                THEN 'Dialysis without nephrology/kidney specialist'
              WHEN eq.equip_lower RLIKE 'cardiac|cardiology'
                AND (cp.cap_text IS NULL OR NOT cp.cap_text RLIKE 'icu|cardiac surgery|cardiac care')
                THEN 'Cardiac equipment without ICU/cardiac surgery support'
              ELSE NULL
            END,
          'severity', 'high'
        )))
      )
    )
  )
  FROM equip eq
  LEFT JOIN caps cp ON eq.facility_id = cp.facility_id
  WHERE eq.equip_lower RLIKE 'mri|ct scan|dialysis|cardiac'
    AND (
      (eq.equip_lower RLIKE 'mri' AND (cp.cap_text IS NULL OR NOT cp.cap_text RLIKE 'radiology|radiographer|mri'))
      OR (eq.equip_lower RLIKE 'ct scan' AND (cp.cap_text IS NULL OR NOT cp.cap_text RLIKE 'radiology|radiographer|ct'))
      OR (eq.equip_lower RLIKE 'dialysis' AND (cp.cap_text IS NULL OR NOT cp.cap_text RLIKE 'nephrology|dialysis|kidney'))
      OR (eq.equip_lower RLIKE 'cardiac' AND (cp.cap_text IS NULL OR NOT cp.cap_text RLIKE 'icu|cardiac'))
    )
)

-- 6. Unmet Needs / Regional Gaps
WHEN LOWER(parse_json(query_json):query) RLIKE 'unmet|gap|need|service gap'
THEN (
  WITH region_services AS (
    SELECT
      ri.state AS region,
      MAX(CASE WHEN LOWER(ri.insight_value) RLIKE 'dialysis|nephrology|kidney' THEN 1 ELSE 0 END) AS has_dialysis,
      MAX(CASE WHEN LOWER(ri.insight_value) RLIKE 'cardiac|cardiology|heart' THEN 1 ELSE 0 END) AS has_cardiac,
      MAX(CASE WHEN LOWER(ri.insight_value) RLIKE 'neonatal|nicu|newborn' THEN 1 ELSE 0 END) AS has_neonatal,
      MAX(CASE WHEN LOWER(ri.insight_value) RLIKE 'mri|magnetic resonance' THEN 1 ELSE 0 END) AS has_mri,
      MAX(CASE WHEN LOWER(ri.insight_value) RLIKE 'cancer|oncology|chemotherapy' THEN 1 ELSE 0 END) AS has_cancer,
      MAX(CASE WHEN LOWER(ri.insight_value) RLIKE 'trauma|orthopaedic surgery' THEN 1 ELSE 0 END) AS has_trauma
    FROM med_atlas_ai.default.regional_insights ri
    WHERE ri.state IS NOT NULL
    GROUP BY ri.state
  ),
  gaps AS (
    SELECT region, 'dialysis' AS service, has_dialysis = 0 AS is_gap, 'critical' AS severity FROM region_services
    UNION ALL SELECT region, 'cardiac', has_cardiac = 0, 'critical' FROM region_services
    UNION ALL SELECT region, 'neonatal_icu', has_neonatal = 0, 'critical' FROM region_services
    UNION ALL SELECT region, 'mri_imaging', has_mri = 0, 'moderate' FROM region_services
    UNION ALL SELECT region, 'cancer_care', has_cancer = 0, 'moderate' FROM region_services
    UNION ALL SELECT region, 'trauma_surgery', has_trauma = 0, 'moderate' FROM region_services
  )
  SELECT to_json(
    map_from_arrays(
      array('query', 'findings'),
      array(
        parse_json(query_json):query,
        to_json(array_agg(named_struct(
          'type', 'unmet_needs',
          'service', service,
          'region', region,
          'severity', severity,
          'recommendation', 'No ' || service || ' services found in ' || region
        )))
      )
    )
  )
  FROM gaps
  WHERE is_gap = TRUE
)

-- 7. Duplicate Facilities (name prefix similarity)
WHEN LOWER(parse_json(query_json):query) RLIKE 'duplicate|duplicat'
THEN (
  WITH namedup AS (
    SELECT
      fr1.facility_id AS fid1,
      fr1.facility_name AS name1,
      fr2.facility_id AS fid2,
      fr2.facility_name AS name2,
      ROW_NUMBER() OVER (
        PARTITION BY
          LOWER(SPLIT(fr1.facility_name, ' ')[0]) || ' ' ||
          LOWER(SPLIT(fr1.facility_name, ' ')[1])
        ORDER BY fr1.facility_id
      ) AS rn
    FROM med_atlas_ai.default.facility_records fr1
    JOIN med_atlas_ai.default.facility_records fr2
      ON LOWER(SPLIT(fr1.facility_name, ' ')[0]) = LOWER(SPLIT(fr2.facility_name, ' ')[0])
     AND LOWER(SPLIT(fr1.facility_name, ' ')[1]) = LOWER(SPLIT(fr2.facility_name, ' ')[1])
     AND fr1.facility_id < fr2.facility_id
  )
  SELECT to_json(
    map_from_arrays(
      array('query', 'findings'),
      array(
        parse_json(query_json):query,
        to_json(array_agg(named_struct(
          'type', 'duplicate_facility',
          'facility_1_id', fid1,
          'facility_1_name', name1,
          'facility_2_id', fid2,
          'facility_2_name', name2,
          'severity', 'medium'
        )))
      )
    )
  )
  FROM namedup
  WHERE rn = 1
)

-- 8. Anomaly Flagging (capacity outliers)
WHEN LOWER(parse_json(query_json):query) RLIKE 'outlier|anomal|flag|unusual|abnormal'
THEN (
  WITH cap_stats AS (
    SELECT
      AVG(CAST(capacity AS DOUBLE)) AS m,
      STDDEV(CAST(capacity AS DOUBLE)) AS s
    FROM med_atlas_ai.default.facility_records
    WHERE capacity IS NOT NULL AND CAST(capacity AS INT) > 0
  ),
  outliers AS (
    SELECT
      fr.facility_id,
      fr.facility_name,
      COALESCE(fr.facility_type, 'unknown') AS facility_type,
      CAST(fr.capacity AS INT) AS value,
      ROUND(cs.m, 1) AS mean,
      ROUND(cs.s, 1) AS std,
      'capacity' AS field,
      'high' AS severity,
      'Outlier: ' || fr.capacity || ' bed capacity (mean=' || ROUND(cs.m, 0) || ')' AS reason
    FROM med_atlas_ai.default.facility_records fr, cap_stats cs
    WHERE fr.capacity IS NOT NULL AND CAST(fr.capacity AS INT) > 0
      AND (
        CAST(fr.capacity AS DOUBLE) < cs.m - 3 * cs.s
        OR CAST(fr.capacity AS DOUBLE) > cs.m + 3 * cs.s
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

-- Fallback
ELSE to_json(
  map_from_arrays(
    array('query', 'findings'),
    array(
      parse_json(query_json):query,
      to_json(array(named_struct(
        'type', 'general',
        'message', 'Query recognized but no specific analysis triggered.',
        'hint', 'Supported: anomalies, reliability score, NGO classification, over-claiming, equipment mismatch, unmet needs, duplicates, outliers'
      )))
    )
  )
)

END
