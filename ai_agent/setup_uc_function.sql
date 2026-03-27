-- =========================================================================
-- Medical Agent UC Function
-- med_atlas_ai.default.analyze_medical_query
-- =========================================================================
-- Pure SQL — works on all Databricks warehouse types.
--
-- Schema (from IDP/storage/models.py):
--   facility_records: facility_id, facility_name, organization_type ('facility'|'ngo'),
--     specialties[], procedures[], equipment[], capabilities[],
--     city, state, country, country_code, number_doctors, capacity,
--     operator_type ('public'|'private'), facility_type ('hospital'|'clinic'|'pharmacy'|'doctor'|'dentist'),
--     affiliation_types[], websites[], description, ...
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
              WHEN LOWER(facility_name) RLIKE 'ngo|mission|charity|catholic|methodist|islamic|red cross|unicef|who|church'
                THEN 'direct_operator'
              WHEN affil_str != '' AND affil_str IS NOT NULL
                AND affil_str = 'faith-tradition'
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
    WHERE organization_type = 'ngo'
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
    WHERE fr.organization_type = 'facility'
      AND LOWER(COALESCE(fr.facility_type, '')) IN ('clinic', 'dentist', 'pharmacy')
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
  WHERE eq.equip_lower RLIKE 'mri|ct scan|dialysis|dialysis center|cardiac'
    AND (
      (eq.equip_lower RLIKE 'mri' AND (cp.cap_text IS NULL OR NOT cp.cap_text RLIKE 'radiology|radiographer|mri'))
      OR (eq.equip_lower RLIKE 'ct scan' AND (cp.cap_text IS NULL OR NOT cp.cap_text RLIKE 'radiology|radiographer|ct'))
      OR (eq.equip_lower RLIKE 'dialysis|dialysis center' AND (cp.cap_text IS NULL OR NOT cp.cap_text RLIKE 'nephrology|dialysis|kidney'))
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
  WITH prepped AS (
    SELECT 
      facility_id,
      facility_name,
      -- Extract the first true alphanumeric word as an efficient join key
      get(split(trim(regexp_replace(lower(facility_name), '[^a-z0-9]+', ' ')), ' '), 0) AS first_word,
      -- Aggressively strip generic medical words, spaces, and punctuation for fuzzy matching
      regexp_replace(
        lower(facility_name), 
        '\\b(hospital|clinic|center|centre|and|health|care|medical|diagnostics?|services|agency|the|of|for|trust|complex|practice|polyclinic|maternity|home|herbal|foundation)\\b|[^a-z0-9]', 
        ''
      ) AS norm_name
    FROM med_atlas_ai.default.facility_records
    WHERE organization_type = 'facility'
  ),
  namedup AS (
    SELECT
      fr1.facility_id AS fid1,
      fr1.facility_name AS name1,
      fr2.facility_id AS fid2,
      fr2.facility_name AS name2,
      ROW_NUMBER() OVER (PARTITION BY fr1.facility_id ORDER BY fr2.facility_id) AS rn
    FROM prepped fr1
    JOIN prepped fr2
      ON fr1.first_word = fr2.first_word 
     AND fr1.first_word IS NOT NULL AND fr1.first_word != ''
     AND length(fr1.norm_name) >= 5 AND length(fr2.norm_name) >= 5
     AND (
         startswith(fr1.norm_name, fr2.norm_name)
         OR 
         startswith(fr2.norm_name, fr1.norm_name)
     )
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

-- 8. Abnormal Ratios (bed/doctor/OR implausibility) — must come before branch 9
-- because branch 9 also matches "abnormal" and we want the specific ratio check first.
WHEN LOWER(parse_json(query_json):query) RLIKE 'abnormal|ratio|bed.to|beds.to|doctor.to|bedper|beds per|ratio vs'
THEN (
  WITH ratio_stats AS (
    SELECT
      fr.facility_id,
      fr.facility_name,
      COALESCE(fr.capacity, 0) AS beds,
      COALESCE(fr.number_doctors, 0) AS doctors,
      CASE WHEN fr.capacity IS NOT NULL AND fr.capacity > 0 AND fr.number_doctors IS NOT NULL AND fr.number_doctors > 0
           THEN ROUND(CAST(fr.capacity AS DOUBLE) / fr.number_doctors, 1)
           ELSE NULL END AS bed_per_doctor
    FROM med_atlas_ai.default.facility_records fr
    WHERE fr.organization_type = 'facility'
      AND fr.facility_type IN ('hospital', 'clinic')
  ),
  stats AS (
    SELECT
      AVG(bed_per_doctor) AS m_ratio,
      STDDEV(bed_per_doctor) AS s_ratio
    FROM ratio_stats
    WHERE bed_per_doctor IS NOT NULL
  )
  SELECT to_json(
    map_from_arrays(
      array('query', 'findings'),
      array(
        parse_json(query_json):query,
        to_json(array_agg(named_struct(
          'type', 'abnormal_ratio',
          'facility_id', facility_id,
          'facility_name', facility_name,
          'beds', beds,
          'doctors', doctors,
          'bed_per_doctor', bed_per_doctor,
          'issue',
            CASE
              WHEN bed_per_doctor < (SELECT m_ratio - 3 * s_ratio FROM stats WHERE s_ratio > 0)
                THEN 'Abnormally low beds-per-doctor ratio — possible understaffing'
              WHEN bed_per_doctor > (SELECT m_ratio + 3 * s_ratio FROM stats WHERE s_ratio > 0)
                THEN 'Abnormally high beds-per-doctor ratio — possible data error'
              ELSE NULL
            END,
          'severity', 'high'
        )))
      )
    )
  )
  FROM ratio_stats
  WHERE bed_per_doctor IS NOT NULL
    AND (
      bed_per_doctor < (SELECT m_ratio - 3 * s_ratio FROM stats WHERE s_ratio > 0)
      OR bed_per_doctor > (SELECT m_ratio + 3 * s_ratio FROM stats WHERE s_ratio > 0)
    )
)

-- 9. Anomaly Flagging (capacity outliers)
WHEN LOWER(parse_json(query_json):query) RLIKE 'outlier|anomal|flag|unusual'
  AND LOWER(parse_json(query_json):query) NOT RLIKE 'abnormal|ratio'
THEN (
  WITH cap_stats AS (
    SELECT
      AVG(CAST(capacity AS DOUBLE)) AS m,
      STDDEV(CAST(capacity AS DOUBLE)) AS s
    FROM med_atlas_ai.default.facility_records
    WHERE organization_type = 'facility'
      AND capacity IS NOT NULL AND CAST(capacity AS INT) > 0
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
    WHERE fr.organization_type = 'facility'
      AND fr.capacity IS NOT NULL AND CAST(fr.capacity AS INT) > 0
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

-- 10. Feature Mismatch (procedure count vs equipment count)
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
          'type', 'feature_mismatch',
          'facility_id', p.facility_id,
          'facility_name', fr.facility_name,
          'n_procedures', p.n_procedures,
          'n_equipment', COALESCE(e.n_equipment, 0),
          'ratio', ROUND(CAST(p.n_procedures AS DOUBLE) / GREATEST(e.n_equipment, 1), 2),
          'issue',
            CASE
              WHEN p.n_procedures > 5 AND COALESCE(e.n_equipment, 0) = 0
                THEN 'Many procedures claimed but no equipment records'
              WHEN p.n_procedures > 3 AND COALESCE(e.n_equipment, 0) = 0
                THEN 'Multiple procedures but zero equipment listed — data quality concern'
              WHEN p.n_procedures > 2 * COALESCE(e.n_equipment, 0)
                THEN 'High procedure-to-equipment ratio — verify capability claims'
              ELSE NULL
            END,
          'severity', 'medium'
        )))
      )
    )
  )
  FROM proc_count p
  JOIN med_atlas_ai.default.facility_records fr ON p.facility_id = fr.facility_id
  LEFT JOIN equip_count e ON p.facility_id = e.facility_id
  WHERE p.n_procedures > 3 AND COALESCE(e.n_equipment, 0) = 0
)

-- 11. Subspecialty vs Infrastructure Mismatch
WHEN LOWER(parse_json(query_json):query) RLIKE 'subspecialty|infrastructure mismatch|specialty vs infra|specialty mismatch'
THEN (
  WITH specialty_caps AS (
    SELECT
      fr.facility_id,
      fr.facility_name,
      COALESCE(fr.facility_type, 'unknown') AS facility_type,
      COALESCE(CAST(fr.capacity AS INT), 0) AS capacity,
      MAX(CASE WHEN LOWER(ff.fact_text) RLIKE 'neurosurgery|neuro|spine|interventional neurology'
               THEN 1 ELSE 0 END) AS has_neuro,
      MAX(CASE WHEN LOWER(ff.fact_text) RLIKE 'cardiac surgery|heart surgery|open heart|cardiothoracic'
               THEN 1 ELSE 0 END) AS has_cardiac_surg,
      MAX(CASE WHEN LOWER(ff.fact_text) RLIKE 'oncology|cancer'
               THEN 1 ELSE 0 END) AS has_oncology,
      MAX(CASE WHEN LOWER(ff.fact_text) RLIKE 'transplant|organ transplant'
               THEN 1 ELSE 0 END) AS has_transplant
    FROM med_atlas_ai.default.facility_records fr
    LEFT JOIN med_atlas_ai.default.facility_facts ff ON fr.facility_id = ff.facility_id
    WHERE fr.organization_type = 'facility'
      AND ff.fact_type IN ('specialty', 'capability')
    GROUP BY fr.facility_id, fr.facility_name, fr.facility_type, fr.capacity
  )
  SELECT to_json(
    map_from_arrays(
      array('query', 'findings'),
      array(
        parse_json(query_json):query,
        to_json(array_agg(named_struct(
          'type', 'specialty_infrastructure_mismatch',
          'facility_id', facility_id,
          'facility_name', facility_name,
          'facility_type', facility_type,
          'capacity', capacity,
          'has_neurosurgery', has_neuro,
          'has_cardiac_surgery', has_cardiac_surg,
          'has_oncology', has_oncology,
          'has_transplant', has_transplant,
          'issue',
            CASE
              WHEN (has_neuro = 1 OR has_cardiac_surg = 1 OR has_oncology = 1 OR has_transplant = 1)
                   AND (facility_type = 'clinic' OR (capacity < 100 AND capacity > 0))
                THEN 'Advanced subspecialty claimed at low-capacity or clinic-level facility'
              WHEN (has_neuro = 1 OR has_cardiac_surg = 1 OR has_oncology = 1 OR has_transplant = 1)
                   AND capacity = 0
                THEN 'Subspecialty service claimed but no bed capacity recorded'
              ELSE NULL
            END,
          'severity', 'high'
        )))
      )
    )
  )
  FROM specialty_caps
  WHERE has_neuro = 1 OR has_cardiac_surg = 1 OR has_oncology = 1 OR has_transplant = 1
)

-- 12. Oversupply vs Scarcity (procedure frequency vs facility count)
WHEN LOWER(parse_json(query_json):query) RLIKE 'oversupply|scarcity|procedure frequency|supply demand|how many facilities'
THEN (
  WITH proc_facilities AS (
    SELECT
      LOWER(ff.fact_text) AS procedure_name,
      COUNT(DISTINCT ff.facility_id) AS n_facilities
    FROM med_atlas_ai.default.facility_facts ff
    WHERE ff.fact_type = 'procedure'
    GROUP BY LOWER(ff.fact_text)
    HAVING COUNT(DISTINCT ff.facility_id) > 0
  ),
  total_facilities AS (
    SELECT COUNT(DISTINCT facility_id) AS total FROM med_atlas_ai.default.facility_records
    WHERE organization_type = 'facility'
  )
  SELECT to_json(
    map_from_arrays(
      array('query', 'findings'),
      array(
        parse_json(query_json):query,
        to_json(array_agg(named_struct(
          'type', 'oversupply_scarcity',
          'procedure', REGEXP_REPLACE(procedure_name, '[^a-zA-Z ]', ''),
          'n_facilities', n_facilities,
          'total_facilities', (SELECT total FROM total_facilities LIMIT 1),
          'penetration_pct', ROUND(100.0 * n_facilities / (SELECT total FROM total_facilities LIMIT 1), 1),
          'classification',
            CASE
              WHEN n_facilities >= 3 THEN 'oversupply'
              WHEN n_facilities = 1 THEN 'scarce'
              ELSE 'adequate'
            END,
          'severity', CASE WHEN n_facilities = 1 THEN 'high' WHEN n_facilities >= 3 THEN 'low' ELSE 'low' END
        )))
      )
    )
  )
  FROM proc_facilities
  WHERE n_facilities <= 3
)

-- 13. NGO Overlap (overlapping NGO presence in same region)
WHEN LOWER(parse_json(query_json):query) RLIKE 'ngo overlap|overlapping ngo|same ngo|same region'
THEN (
  WITH ngo_facilities AS (
    SELECT DISTINCT
      fr.facility_id,
      fr.facility_name,
      fr.state,
      fr.city,
      fr.operator_type,
      fr.affiliation_types,
      CASE
        WHEN LOWER(fr.facility_name) RLIKE 'ngo|mission|charity|catholic|methodist|islamic|red cross|unicef|who|church'
          THEN fr.facility_name
        WHEN fr.affiliation_types IS NOT NULL
          THEN ARRAY_JOIN(fr.affiliation_types, ',')
        ELSE NULL
      END AS ngo_keywords
    FROM med_atlas_ai.default.facility_records fr
    WHERE fr.organization_type = 'ngo'
      AND (LOWER(fr.facility_name) RLIKE 'ngo|mission|charity|catholic|methodist|islamic|red cross|unicef|who|church'
       OR (fr.affiliation_types IS NOT NULL AND ARRAY_JOIN(fr.affiliation_types, ',') = 'faith-tradition'))
  ),
  overlaps AS (
    SELECT
      a.state AS region,
      a.ngo_keywords AS ngo_name,
      COUNT(*) AS n_facilities,
      COLLECT_LIST(a.facility_name) AS facility_list
    FROM ngo_facilities a
    WHERE a.state IS NOT NULL AND a.ngo_keywords IS NOT NULL
    GROUP BY a.state, a.ngo_keywords
    HAVING COUNT(*) > 1
  )
  SELECT to_json(
    map_from_arrays(
      array('query', 'findings'),
      array(
        parse_json(query_json):query,
        to_json(array_agg(named_struct(
          'type', 'ngo_overlap',
          'region', region,
          'ngo_name', ngo_name,
          'n_facilities', n_facilities,
          'facilities', facility_list,
          'issue', 'Multiple facilities from the same NGO operating in the same region',
          'severity', 'low'
        )))
      )
    )
  )
  FROM overlaps
)

-- 14. Problem Type Classification (equipment/training/workforce gap analysis)
WHEN LOWER(parse_json(query_json):query) RLIKE 'problem type|root cause|gap type|classify gap|workforce|staffing|equipment gap|staff shortage'
THEN (
  WITH gap_analysis AS (
    SELECT
      fr.facility_id,
      fr.facility_name,
      fr.state,
      fr.city,
      COALESCE(fr.facility_type, 'unknown') AS facility_type,
      COALESCE(fr.number_doctors, 0) AS doctors,
      fr.number_doctors IS NULL AS has_no_doctor_record,
      COALESCE(CAST(fr.capacity AS INT), 0) AS beds,
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
          'type', 'problem_type',
          'facility_id', facility_id,
          'facility_name', facility_name,
          'state', state,
          'doctors', doctors,
          'beds', beds,
          'equipment_count', n_equip,
          'specialty_count', n_specialties,
          'procedure_count', n_procedures,
          'problem_category',
            CASE
              WHEN n_equip = 0 AND n_specialties > 0 AND facility_type IN ('clinic', 'pharmacy')
                THEN 'equipment_gap'
              WHEN (doctors = 0 OR has_no_doctor_record = TRUE) AND (n_specialties > 0 OR n_procedures > 0)
                THEN 'workforce_gap'
              WHEN n_specialties > 5 AND doctors < 3 AND facility_type IN ('clinic', 'pharmacy')
                THEN 'training_gap'
              WHEN n_equip > 0 AND n_specialties = 0 AND n_procedures = 0
                THEN 'service_gap'
              WHEN facility_type = 'clinic' AND (n_specialties > 3 OR n_procedures > 5)
                THEN 'overclaim_gap'
              ELSE 'balanced'
            END,
          'recommendation',
            CASE
              WHEN n_equip = 0 AND n_specialties > 0 AND facility_type IN ('clinic', 'pharmacy')
                THEN 'Clinic/pharmacy claims specialty services but has no equipment records — verify inventory'
              WHEN (doctors = 0 OR has_no_doctor_record = TRUE) AND (n_specialties > 0 OR n_procedures > 0)
                THEN 'Specialties/procedures claimed but no doctors on record — workforce verification needed'
              WHEN n_specialties > 5 AND doctors < 3 AND facility_type IN ('clinic', 'pharmacy')
                THEN 'Many specialties claimed at a small facility with very few doctors — training capacity unlikely'
              WHEN n_equip > 0 AND n_specialties = 0 AND n_procedures = 0
                THEN 'Equipment present but no services documented — possible capability gap'
              WHEN facility_type = 'clinic' AND (n_specialties > 3 OR n_procedures > 5)
                THEN 'Clinic claiming many services — verify scope matches facility type'
              ELSE 'No significant gap pattern detected'
            END,
          'severity',
            CASE
              WHEN (doctors = 0 OR has_no_doctor_record = TRUE) AND (n_specialties > 0 OR n_procedures > 0)
                THEN 'high'
              WHEN n_equip = 0 AND n_specialties > 0 AND facility_type IN ('clinic', 'pharmacy')
                THEN 'high'
              WHEN facility_type = 'clinic' AND (n_specialties > 3 OR n_procedures > 5)
                THEN 'medium'
              ELSE 'low'
            END
        )))
      )
    )
  )
  FROM gap_analysis
  WHERE
    (n_equip = 0 AND n_specialties > 0 AND facility_type IN ('clinic', 'pharmacy'))
    OR ((doctors = 0 OR has_no_doctor_record = TRUE) AND (n_specialties > 0 OR n_procedures > 0))
    OR (n_specialties > 5 AND doctors < 3 AND facility_type IN ('clinic', 'pharmacy'))
    OR (n_equip > 0 AND n_specialties = 0 AND n_procedures = 0)
    OR (facility_type = 'clinic' AND (n_specialties > 3 OR n_procedures > 5))
)

-- 15. Specialist Distribution (specialty → region mapping)
WHEN LOWER(parse_json(query_json):query) RLIKE 'specialist|specialist distribution|specialty region|specialists practicing'
THEN (
  WITH specialist_regions AS (
    SELECT
      fr.state AS region,
      LOWER(ff.fact_text) AS specialty,
      COUNT(DISTINCT fr.facility_id) AS n_facilities,
      COLLECT_LIST(fr.facility_name) AS facilities
    FROM med_atlas_ai.default.facility_records fr
    JOIN med_atlas_ai.default.facility_facts ff ON fr.facility_id = ff.facility_id
    WHERE fr.organization_type = 'facility'
      AND fr.state IS NOT NULL
      AND ff.fact_type = 'specialty'
      AND LOWER(ff.fact_text) RLIKE
        'cardiology|cardiac surgery|'
        || 'cardiacsurgery|'
        || 'neurology|neurosurgery|neuro|'
        || 'oncology|medicaloncology|cancer|'
        || 'pediatrics|pediatric|paediatric|peds|'
        || 'ophthalmology|eye|'
        || 'nephrology|kidney|'
        || 'orthopedic|orthopaedic|orthopedicsurgery|'
        || 'gynecologyandobstetrics|obstetrics|obgyn|maternity|women|'
        || 'radiology|imaging|x-ray|ultrasound|'
        || 'infectiousdiseases|infectious disease|'
        || 'anesthesia|anesthesiology|'
        || 'geriatrics|elderly|internal medicine|'
        || 'endocrinology|diabetes|metabolism|'
        || 'otolaryngology|ent|ear nose|throat|'
        || 'physicalmedicine|rehab|pmr|physiatry|'
        || 'plasticsurgery|plastic surgery|cosmetic|'
        || 'neonatology|nicu|neonatal|newborn|'
        || 'dentistry|dental|tooth|'
        || 'pathology|laboratory|diagnostic lab|'
        || 'familymedicine|family medicine|'
        || 'emergency medicine|emergency|er|ed|'
        || 'generalsurgery|general surgery|surgery|'
        || 'critical care|intensive care|icu|'
        || 'hospice|palliative|'
        || 'medical oncology|'
        || 'orthodontics|orthodontic'
    GROUP BY fr.state, LOWER(ff.fact_text)
  )
  SELECT to_json(
    map_from_arrays(
      array('query', 'findings'),
      array(
        parse_json(query_json):query,
        to_json(array_agg(named_struct(
          'type', 'specialist_distribution',
          'region', region,
          'specialty', specialty,
          'n_facilities', n_facilities,
          'facilities', facilities,
          'severity',
            CASE WHEN n_facilities = 1 THEN 'high'
                 WHEN n_facilities = 2 THEN 'medium'
                 ELSE 'low' END
        )))
      )
    )
  )
  FROM specialist_regions
)

-- 16. Web Capability Mismatch (description reliability vs actual services)
WHEN LOWER(parse_json(query_json):query) RLIKE 'web|website|online presence|description|capability mismatch|web presence'
THEN (
  WITH capability_check AS (
    SELECT
      fr.facility_id,
      fr.facility_name,
      fr.websites,
      fr.description,
      fr.capacity,
      (SELECT COUNT(*) FROM med_atlas_ai.default.facility_facts ff
       WHERE ff.facility_id = fr.facility_id) AS n_facts,
      (SELECT COUNT(*) FROM med_atlas_ai.default.facility_facts ff
       WHERE ff.facility_id = fr.facility_id AND ff.fact_type IN ('specialty', 'procedure')) AS n_services
    FROM med_atlas_ai.default.facility_records fr
    WHERE fr.organization_type = 'facility'
      AND fr.websites IS NOT NULL AND SIZE(fr.websites) > 0
  )
  SELECT to_json(
    map_from_arrays(
      array('query', 'findings'),
      array(
        parse_json(query_json):query,
        to_json(array_agg(named_struct(
          'type', 'web_capability_mismatch',
          'facility_id', facility_id,
          'facility_name', facility_name,
          'has_website', TRUE,
          'description_length', LENGTH(COALESCE(description, '')),
          'n_facts_recorded', n_facts,
          'n_services', n_services,
          'capacity', capacity,
          'issue',
            CASE
              WHEN description IS NOT NULL AND LENGTH(description) > 200 AND n_facts = 0
                THEN 'Detailed online description but no structured data recorded — verify claims'
              WHEN n_facts = 0
                THEN 'Website present but no facility facts on record'
              WHEN n_services = 0 AND n_facts > 0
                THEN 'Facility facts exist but no services/specialties recorded'
              ELSE NULL
            END,
          'severity',
            CASE WHEN n_facts = 0 THEN 'medium' ELSE 'low' END
        )))
      )
    )
  )
  FROM capability_check
  WHERE (description IS NOT NULL AND LENGTH(description) > 200 AND n_facts = 0)
     OR n_facts = 0
     OR (n_services = 0 AND n_facts > 0)
)

-- 17. Visiting vs Permanent Staff (facility_type vs staffing patterns)
WHEN LOWER(parse_json(query_json):query) RLIKE 'visiting|permanent staff|staff type|part time|full time|staffing pattern'
THEN (
  SELECT to_json(
    map_from_arrays(
      array('query', 'findings'),
      array(
        parse_json(query_json):query,
        to_json(array_agg(named_struct(
          'type', 'visiting_vs_permanent',
          'facility_id', fr.facility_id,
          'facility_name', fr.facility_name,
          'facility_type', COALESCE(fr.facility_type, 'unknown'),
          'operator_type', COALESCE(fr.operator_type, 'unknown'),
          'number_doctors', COALESCE(fr.number_doctors, 0),
          'capacity', COALESCE(CAST(fr.capacity AS INT), 0),
          'staffing_pattern',
            CASE
              WHEN fr.facility_type = 'clinic' AND COALESCE(fr.number_doctors, 0) = 0
                THEN 'likely_visiting'
              WHEN fr.facility_type = 'clinic' AND COALESCE(fr.number_doctors, 0) BETWEEN 1 AND 2
                THEN 'minimal_permanent'
              WHEN fr.facility_type = 'hospital' AND COALESCE(fr.number_doctors, 0) >= 5
                THEN 'permanent_staff'
              WHEN fr.facility_type = 'hospital' AND COALESCE(fr.number_doctors, 0) BETWEEN 1 AND 4
                THEN 'minimal_permanent_may_use_visiting'
              ELSE 'unknown'
            END,
          'issue',
            CASE
              WHEN fr.facility_type = 'clinic' AND COALESCE(fr.number_doctors, 0) = 0
                THEN 'Clinic with no doctors on record — likely relies on visiting specialists'
              WHEN fr.facility_type = 'hospital' AND COALESCE(fr.number_doctors, 0) BETWEEN 1 AND 4
                THEN 'Hospital with very few doctors — possible visiting staff model'
              ELSE NULL
            END,
          'severity',
            CASE
              WHEN fr.facility_type = 'clinic' AND COALESCE(fr.number_doctors, 0) = 0 THEN 'medium'
              WHEN fr.facility_type = 'hospital' AND COALESCE(fr.number_doctors, 0) BETWEEN 1 AND 4 THEN 'medium'
              ELSE 'low'
            END
        )))
      )
    )
  )
  FROM med_atlas_ai.default.facility_records fr
  WHERE fr.organization_type = 'facility'
    AND fr.facility_type IN ('clinic', 'hospital')
)

-- 18. Data Staleness (updated_at age scoring)
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
        'hint', 'Supported: anomalies, reliability score, NGO classification/overlap, over-claiming, equipment mismatch, unmet needs, duplicates, outliers, abnormal ratios, feature mismatch, specialty-infrastructure mismatch, oversupply/scarcity, problem type, specialist distribution, web capability mismatch, visiting vs permanent staff, data staleness'
      )))
    )
  )
)

END;
