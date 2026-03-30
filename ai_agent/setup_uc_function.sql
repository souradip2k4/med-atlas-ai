-- =========================================================================
-- Medical Agent UC Function
-- med_atlas_ai.default.analyze_medical_query
-- =========================================================================
-- Pure SQL — works on all Databricks warehouse types.
--
-- Schema (from IDP/storage/models.py):
--   facility_records: facility_id, facility_name, organization_type ('facility'|'ngo'),
--     specialties[], procedures[], equipment[], capabilities[],
--     city, state, country, country_code, no_beds,
--     total_facts, and more._type ('public'|'private'), facility_type ('hospital'|'clinic'|'pharmacy'|'doctor'|'dentist'),
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
      COALESCE(CAST(fr.no_beds AS INT), 0) AS no_beds,
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
          'no_beds', no_beds
        )))
      )
    )
  )
  FROM scored
  WHERE reliability_score < 75
)


-- 4. Equipment counts vs Procedure counts (Feature Mismatch — moved up, purely numeric)
-- NOTE: Branch 5 (Equipment-Procedure Mismatch via regex) has been removed.
-- Medical domain reasoning about equipment-capability plausibility is now handled
-- by the LLM agent directly using genie_chat_tool + vector_search_tool.

-- 6. Unmet Needs / Regional Gaps
-- Queries facility_records arrays directly (specialties, procedures,
-- capabilities, equipment) instead of regional_insights free-text rows.
WHEN LOWER(parse_json(query_json):query) RLIKE 'unmet|gap|need|service gap'
THEN (
  WITH region_services AS (
    SELECT
      fr.state AS region,
      MAX(CASE WHEN LOWER(
          COALESCE(ARRAY_JOIN(fr.specialties, ' '), '') || ' ' ||
          COALESCE(ARRAY_JOIN(fr.procedures, ' '), '') || ' ' ||
          COALESCE(ARRAY_JOIN(fr.capabilities, ' '), '') || ' ' ||
          COALESCE(ARRAY_JOIN(fr.equipment, ' '), '')
        ) RLIKE 'dialysis|nephrology|kidney' THEN 1 ELSE 0 END) AS has_dialysis,
      MAX(CASE WHEN LOWER(
          COALESCE(ARRAY_JOIN(fr.specialties, ' '), '') || ' ' ||
          COALESCE(ARRAY_JOIN(fr.procedures, ' '), '') || ' ' ||
          COALESCE(ARRAY_JOIN(fr.capabilities, ' '), '') || ' ' ||
          COALESCE(ARRAY_JOIN(fr.equipment, ' '), '')
        ) RLIKE 'cardiac|cardiology|heart' THEN 1 ELSE 0 END) AS has_cardiac,
      MAX(CASE WHEN LOWER(
          COALESCE(ARRAY_JOIN(fr.specialties, ' '), '') || ' ' ||
          COALESCE(ARRAY_JOIN(fr.procedures, ' '), '') || ' ' ||
          COALESCE(ARRAY_JOIN(fr.capabilities, ' '), '') || ' ' ||
          COALESCE(ARRAY_JOIN(fr.equipment, ' '), '')
        ) RLIKE 'neonatal|nicu|newborn' THEN 1 ELSE 0 END) AS has_neonatal,
      MAX(CASE WHEN LOWER(
          COALESCE(ARRAY_JOIN(fr.specialties, ' '), '') || ' ' ||
          COALESCE(ARRAY_JOIN(fr.procedures, ' '), '') || ' ' ||
          COALESCE(ARRAY_JOIN(fr.capabilities, ' '), '') || ' ' ||
          COALESCE(ARRAY_JOIN(fr.equipment, ' '), '')
        ) RLIKE 'mri|magnetic resonance' THEN 1 ELSE 0 END) AS has_mri,
      MAX(CASE WHEN LOWER(
          COALESCE(ARRAY_JOIN(fr.specialties, ' '), '') || ' ' ||
          COALESCE(ARRAY_JOIN(fr.procedures, ' '), '') || ' ' ||
          COALESCE(ARRAY_JOIN(fr.capabilities, ' '), '') || ' ' ||
          COALESCE(ARRAY_JOIN(fr.equipment, ' '), '')
        ) RLIKE 'cancer|oncology|chemotherapy' THEN 1 ELSE 0 END) AS has_cancer,
      MAX(CASE WHEN LOWER(
          COALESCE(ARRAY_JOIN(fr.specialties, ' '), '') || ' ' ||
          COALESCE(ARRAY_JOIN(fr.procedures, ' '), '') || ' ' ||
          COALESCE(ARRAY_JOIN(fr.capabilities, ' '), '') || ' ' ||
          COALESCE(ARRAY_JOIN(fr.equipment, ' '), '')
        ) RLIKE 'trauma|orthopaedic surgery' THEN 1 ELSE 0 END) AS has_trauma
    FROM med_atlas_ai.default.facility_records fr
    WHERE fr.state IS NOT NULL
    GROUP BY fr.state
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

-- 9. Anomaly Flagging (bed count outliers)
WHEN LOWER(parse_json(query_json):query) RLIKE 'outlier|anomal|flag|unusual'
THEN (
  WITH cap_stats AS (
    SELECT
      AVG(CAST(no_beds AS DOUBLE)) AS m,
      STDDEV(CAST(no_beds AS DOUBLE)) AS s
    FROM med_atlas_ai.default.facility_records
    WHERE organization_type = 'facility'
      AND no_beds IS NOT NULL AND CAST(no_beds AS INT) > 0
  ),
  outliers AS (
    SELECT
      fr.facility_id,
      fr.facility_name,
      COALESCE(fr.facility_type, 'unknown') AS facility_type,
      CAST(fr.no_beds AS INT) AS value,
      ROUND(cs.m, 1) AS mean,
      ROUND(cs.s, 1) AS std,
      'no_beds' AS field,
      'high' AS severity,
      'Outlier: ' || fr.no_beds || ' beds (mean=' || ROUND(cs.m, 0) || ')' AS reason
    FROM med_atlas_ai.default.facility_records fr, cap_stats cs
    WHERE fr.organization_type = 'facility'
      AND fr.no_beds IS NOT NULL AND CAST(fr.no_beds AS INT) > 0
      AND (
        CAST(fr.no_beds AS DOUBLE) < cs.m - 3 * cs.s
        OR CAST(fr.no_beds AS DOUBLE) > cs.m + 3 * cs.s
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

-- 11. Subspecialty vs Infrastructure Mismatch — REMOVED
-- This branch has been replaced by LLM-native medical reasoning.
-- The agent now fetches facility profiles via genie_chat_tool and uses its own
-- medical domain knowledge to detect subspecialty-infrastructure mismatches.

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
      COALESCE(CAST(fr.no_beds AS INT), 0) AS beds,
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
          'beds', beds,
          'equipment_count', n_equip,
          'specialty_count', n_specialties,
          'procedure_count', n_procedures,
          'problem_category',
            CASE
              WHEN n_equip = 0 AND n_specialties > 0 AND facility_type IN ('clinic', 'pharmacy')
                THEN 'equipment_gap'
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
              WHEN n_specialties > 0 OR n_procedures > 0
                THEN 'Specialties/procedures claimed — workforce verification recommended'
              WHEN n_equip > 0 AND n_specialties = 0 AND n_procedures = 0
                THEN 'Equipment present but no services documented — possible capability gap'
              WHEN facility_type = 'clinic' AND (n_specialties > 3 OR n_procedures > 5)
                THEN 'Clinic claiming many services — verify scope matches facility type'
              ELSE 'No significant gap pattern detected'
            END,
          'severity',
            CASE
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
    OR (n_specialties > 0 OR n_procedures > 0)
    OR (n_equip > 0 AND n_specialties = 0 AND n_procedures = 0)
    OR (facility_type = 'clinic' AND (n_specialties > 3 OR n_procedures > 5))
)

-- 15. Specialist Distribution (specialty → region mapping)
WHEN LOWER(parse_json(query_json):query) RLIKE 'specialist|specialist distribution|specialty region|specialists practicing'
THEN (
  WITH specialist_regions AS (
    SELECT
      ri.state AS region,
      LOWER(ri.insight_value) AS specialty,
      ri.facility_count AS n_facilities,
      ri.contributing_facility_ids AS facilities
    FROM med_atlas_ai.default.regional_insights ri
    WHERE ri.insight_category = 'specialty'
      AND ri.state IS NOT NULL
      AND LOWER(ri.insight_value) RLIKE
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
      fr.no_beds,
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
          'no_beds', no_beds,
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