-- Step 4: Staging Source 2 (Silver Layer)
-- Normalizes to canonical schema with cleaning and error tracking
-- Join key: linkedin_id (URL slug, matches Source 1's linkedinID)

CREATE OR REPLACE TABLE `coffeespace-sandbox.coffeespace_canonical.stg_source_2` AS

WITH parsed AS (
  SELECT
    *,
    ARRAY<STRUCT<field STRING, error STRING, raw_value STRING>>[] AS _errors
  FROM `coffeespace-sandbox.coffeespace_canonical.raw_source_2`
),

with_errors AS (
  SELECT
    p.*,
    ARRAY_CONCAT(
      p._errors,
      IF(linkedin_id IS NULL,
         [STRUCT('linkedin_id' AS field, 'NULL_VALUE' AS error, '' AS raw_value)],
         []),
      IF(REGEXP_CONTAINS(name, r'\s{2,}'),
         [STRUCT('full_name' AS field, 'EXTRA_WHITESPACE' AS error, name AS raw_value)],
         [])
    ) AS normalization_errors
  FROM parsed p
)

SELECT
  -- Primary key: URL slug (e.g., "john-doe-123")
  linkedin_id,

  -- Identity (canonical struct, with cleaning)
  STRUCT(
    TRIM(REGEXP_REPLACE(name, r'\s+', ' ')) AS full_name,
    TRIM(first_name) AS first_name,
    TRIM(last_name) AS last_name,
    position AS headline,
    about
  ) AS identity,

  -- Identity sources (for provenance)
  [STRUCT(
    'source_2' AS source_system,
    TRIM(REGEXP_REPLACE(name, r'\s+', ' ')) AS full_name,
    position AS headline,
    about,
    CURRENT_TIMESTAMP() AS last_updated
  )] AS identity_sources,

  -- Location (flat in Source 2)
  STRUCT(
    location AS display_string,
    CAST(NULL AS STRING) AS country,
    CAST(NULL AS STRING) AS region,
    city AS locality,
    country_code,
    CAST([] AS ARRAY<INT64>) AS location_ids
  ) AS location,

  -- Social metrics
  STRUCT(
    connections,
    followers,
    CURRENT_TIMESTAMP() AS metrics_as_of
  ) AS social_metrics,

  -- Experience array
  ARRAY(
    SELECT AS STRUCT
      TO_HEX(MD5(CONCAT(
        COALESCE(exp.company_id, COALESCE(exp.company, '')),
        COALESCE(exp.title, ''),
        COALESCE(exp.start_date, '')
      ))) AS experience_id,
      exp.company AS company_name,
      exp.company_id AS company_linkedin_id,
      exp.title,
      SAFE.PARSE_DATE('%b %Y', exp.start_date) AS start_date,
      IF(exp.end_date = 'Present', NULL, SAFE.PARSE_DATE('%b %Y', exp.end_date)) AS end_date,
      exp.location,
      exp.description,
      (exp.end_date IS NULL OR exp.end_date = 'Present') AS is_current,
      'source_2' AS source_system
    FROM UNNEST(experience) AS exp
    WHERE exp IS NOT NULL
  ) AS experience,

  -- Education array
  ARRAY(
    SELECT AS STRUCT
      TO_HEX(MD5(CONCAT(
        COALESCE(edu.title, ''),
        COALESCE(edu.degree, ''),
        ''
      ))) AS education_id,
      edu.title AS institution_name,
      edu.degree,
      edu.field AS field_of_study,
      CAST(NULL AS DATE) AS start_date,
      CAST(NULL AS DATE) AS end_date,
      'source_2' AS source_system
    FROM UNNEST(education) AS edu
    WHERE edu IS NOT NULL
  ) AS education,

  -- Certifications (Source 2 exclusive)
  ARRAY(
    SELECT AS STRUCT
      cert.title,
      cert.subtitle AS issuing_org,
      CAST(NULL AS DATE) AS issue_date,
      cert.credential_id
    FROM UNNEST(certifications) AS cert
    WHERE cert IS NOT NULL
  ) AS certifications,

  -- Skills (not in Source 2 sample, leave empty)
  CAST([] AS ARRAY<STRING>) AS skills,

  -- Computed signals (not in Source 2)
  CAST(NULL AS STRUCT<
    likely_to_explore BOOL,
    recently_left_company BOOL,
    potential_to_leave BOOL,
    prior_backed_founder BOOL,
    unicorn_early_engineer BOOL,
    big_tech_alum_private BOOL,
    big_tech_alum_public BOOL
  >) AS computed_signals,

  -- Provenance
  id AS source_id,
  'source_2' AS source_system,
  CURRENT_TIMESTAMP() AS last_updated,

  -- Error tracking
  normalization_errors

FROM with_errors
WHERE linkedin_id IS NOT NULL;
