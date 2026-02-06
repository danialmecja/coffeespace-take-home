-- Step 5: Merge to Canonical (Gold Layer)
-- FULL OUTER JOIN on linkedin_id (URL slug) handles A-only, B-only, and A+B cases

CREATE OR REPLACE TABLE `coffeespace-sandbox.coffeespace_canonical.people_canonical` AS

SELECT
  -- Primary key (deterministic hash of linkedin_id)
  TO_HEX(MD5(COALESCE(s1.linkedin_id, s2.linkedin_id))) AS canonical_id,
  COALESCE(s1.linkedin_id, s2.linkedin_id) AS linkedin_id,

  -- Identity: prefer Source 1 (higher completeness per Part 1 profiling)
  STRUCT(
    COALESCE(s1.identity.full_name, s2.identity.full_name) AS full_name,
    COALESCE(s1.identity.first_name, s2.identity.first_name) AS first_name,
    COALESCE(s1.identity.last_name, s2.identity.last_name) AS last_name,
    COALESCE(s1.identity.headline, s2.identity.headline) AS headline,
    COALESCE(s1.identity.about, s2.identity.about) AS about
  ) AS identity,

  -- Identity sources: preserve BOTH for provenance (never lose data)
  ARRAY_CONCAT(
    COALESCE(s1.identity_sources, []),
    COALESCE(s2.identity_sources, [])
  ) AS identity_sources,

  -- Location: prefer Source 1 (has hierarchy + location_ids)
  STRUCT(
    COALESCE(s1.location.display_string, s2.location.display_string) AS display_string,
    s1.location.country AS country,
    s1.location.region AS region,
    COALESCE(s1.location.locality, s2.location.locality) AS locality,
    COALESCE(s1.location.country_code, s2.location.country_code) AS country_code,
    s1.location.location_ids AS location_ids
  ) AS location,

  -- Social metrics: prefer most recent
  STRUCT(
    CASE
      WHEN s1.social_metrics.metrics_as_of >= COALESCE(s2.social_metrics.metrics_as_of, TIMESTAMP('1970-01-01'))
      THEN s1.social_metrics.connections
      ELSE COALESCE(s2.social_metrics.connections, s1.social_metrics.connections)
    END AS connections,
    CASE
      WHEN s1.social_metrics.metrics_as_of >= COALESCE(s2.social_metrics.metrics_as_of, TIMESTAMP('1970-01-01'))
      THEN s1.social_metrics.followers
      ELSE COALESCE(s2.social_metrics.followers, s1.social_metrics.followers)
    END AS followers,
    GREATEST(
      COALESCE(s1.social_metrics.metrics_as_of, TIMESTAMP('1970-01-01')),
      COALESCE(s2.social_metrics.metrics_as_of, TIMESTAMP('1970-01-01'))
    ) AS metrics_as_of
  ) AS social_metrics,

  -- Experience: UNION from both sources, dedupe by experience_id
  (
    SELECT ARRAY_AGG(STRUCT(
      experience_id,
      company_name,
      company_linkedin_id,
      title,
      start_date,
      end_date,
      location,
      description,
      is_current,
      source_system
    ))
    FROM (
      SELECT DISTINCT
        exp.experience_id,
        exp.company_name,
        exp.company_linkedin_id,
        exp.title,
        exp.start_date,
        exp.end_date,
        exp.location,
        exp.description,
        exp.is_current,
        exp.source_system
      FROM UNNEST(ARRAY_CONCAT(
        COALESCE(s1.experience, []),
        COALESCE(s2.experience, [])
      )) AS exp
    )
  ) AS experience,

  -- Education: UNION from both sources, dedupe by education_id
  (
    SELECT ARRAY_AGG(STRUCT(
      education_id,
      institution_name,
      degree,
      field_of_study,
      start_date,
      end_date,
      source_system
    ))
    FROM (
      SELECT DISTINCT
        edu.education_id,
        edu.institution_name,
        edu.degree,
        edu.field_of_study,
        edu.start_date,
        edu.end_date,
        edu.source_system
      FROM UNNEST(ARRAY_CONCAT(
        COALESCE(s1.education, []),
        COALESCE(s2.education, [])
      )) AS edu
    )
  ) AS education,

  -- Certifications: Source 2 only
  COALESCE(s2.certifications, []) AS certifications,

  -- Skills: UNION and dedupe
  (
    SELECT ARRAY_AGG(DISTINCT skill)
    FROM UNNEST(ARRAY_CONCAT(
      COALESCE(s1.skills, []),
      COALESCE(s2.skills, [])
    )) AS skill
    WHERE skill IS NOT NULL
  ) AS skills,

  -- Computed signals: Source 1 only
  s1.computed_signals,

  -- Derived fields (placeholder, computed in Step 6)
  STRUCT(
    CAST(NULL AS STRING) AS primary_portfolio,
    CAST(NULL AS FLOAT64) AS years_of_experience,
    CAST(NULL AS STRING) AS computation_method
  ) AS derived_fields,

  -- Provenance
  STRUCT(
    CASE
      WHEN s1.linkedin_id IS NOT NULL AND s2.linkedin_id IS NOT NULL
        THEN ['source_1', 'source_2']
      WHEN s1.linkedin_id IS NOT NULL
        THEN ['source_1']
      ELSE ['source_2']
    END AS source_systems,
    s1.source_id AS source_1_id,
    s2.source_id AS source_2_id,
    s1.last_updated AS source_1_last_updated,
    s2.last_updated AS source_2_last_updated,
    CURRENT_TIMESTAMP() AS first_seen_at,
    CURRENT_TIMESTAMP() AS last_merged_at,
    1 AS record_version
  ) AS provenance,

  -- Sync metadata (for Part 4)
  STRUCT(
    COALESCE(s1.linkedin_id, s2.linkedin_id) AS firestore_doc_id,
    CAST(NULL AS TIMESTAMP) AS last_synced_at,
    CAST(NULL AS STRING) AS sync_hash
  ) AS sync_metadata,

  -- Normalization errors: UNION from both sources
  ARRAY_CONCAT(
    COALESCE(s1.normalization_errors, []),
    COALESCE(s2.normalization_errors, [])
  ) AS normalization_errors

FROM `coffeespace-sandbox.coffeespace_canonical.stg_source_1` s1
FULL OUTER JOIN `coffeespace-sandbox.coffeespace_canonical.stg_source_2` s2
  ON s1.linkedin_id = s2.linkedin_id;
