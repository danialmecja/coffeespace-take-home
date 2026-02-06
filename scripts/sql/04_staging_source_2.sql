-- Step 4: Staging Source 2 (Silver Layer)
-- Normalizes to canonical schema with cleaning and error tracking
-- Join key: linkedin_id (URL slug, matches Source 1's linkedinID)
--
-- NOTE: Source 2 is loaded as raw JSON strings (single json_line column)
-- because autodetect fails on dirty data (e.g., end_year = "2022-05" vs 2022)
-- This approach parses fields safely using JSON_VALUE/JSON_QUERY_ARRAY.

CREATE OR REPLACE TABLE `coffeespace-sandbox.coffeespace_canonical.stg_source_2` AS

WITH parsed AS (
  SELECT
    json_line,
    -- Extract scalar fields
    JSON_VALUE(json_line, '$.linkedin_id') AS linkedin_id,
    JSON_VALUE(json_line, '$.id') AS id,
    JSON_VALUE(json_line, '$.name') AS name,
    JSON_VALUE(json_line, '$.first_name') AS first_name,
    JSON_VALUE(json_line, '$.last_name') AS last_name,
    JSON_VALUE(json_line, '$.position') AS position,
    JSON_VALUE(json_line, '$.about') AS about,
    JSON_VALUE(json_line, '$.location') AS location,
    JSON_VALUE(json_line, '$.city') AS city,
    JSON_VALUE(json_line, '$.country_code') AS country_code,
    SAFE_CAST(JSON_VALUE(json_line, '$.connections') AS INT64) AS connections,
    SAFE_CAST(JSON_VALUE(json_line, '$.followers') AS INT64) AS followers,
    -- Arrays (kept as JSON for further parsing)
    JSON_QUERY(json_line, '$.experience') AS experience_json,
    JSON_QUERY(json_line, '$.education') AS education_json,
    JSON_QUERY(json_line, '$.certifications') AS certifications_json,
    ARRAY<STRUCT<field STRING, error STRING, raw_value STRING>>[] AS _errors
  FROM `coffeespace-sandbox.coffeespace_canonical.raw_source_2_sample50`
),

with_errors AS (
  SELECT
    p.*,
    ARRAY_CONCAT(
      p._errors,
      IF(p.linkedin_id IS NULL,
         [STRUCT('linkedin_id' AS field, 'NULL_VALUE' AS error, '' AS raw_value)],
         []),
      IF(REGEXP_CONTAINS(p.name, r'\s{2,}'),
         [STRUCT('full_name' AS field, 'EXTRA_WHITESPACE' AS error, p.name AS raw_value)],
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

  -- Experience array (parse from JSON)
  ARRAY(
    SELECT AS STRUCT
      TO_HEX(MD5(CONCAT(
        COALESCE(JSON_VALUE(exp, '$.company_id'), COALESCE(JSON_VALUE(exp, '$.company'), '')),
        COALESCE(JSON_VALUE(exp, '$.title'), ''),
        COALESCE(JSON_VALUE(exp, '$.start_date'), '')
      ))) AS experience_id,
      JSON_VALUE(exp, '$.company') AS company_name,
      JSON_VALUE(exp, '$.company_id') AS company_linkedin_id,
      JSON_VALUE(exp, '$.title') AS title,
      SAFE.PARSE_DATE('%b %Y', JSON_VALUE(exp, '$.start_date')) AS start_date,
      IF(JSON_VALUE(exp, '$.end_date') = 'Present', NULL,
         SAFE.PARSE_DATE('%b %Y', JSON_VALUE(exp, '$.end_date'))) AS end_date,
      JSON_VALUE(exp, '$.location') AS location,
      JSON_VALUE(exp, '$.description') AS description,
      (JSON_VALUE(exp, '$.end_date') IS NULL OR JSON_VALUE(exp, '$.end_date') = 'Present') AS is_current,
      'source_2' AS source_system
    FROM UNNEST(JSON_QUERY_ARRAY(experience_json)) AS exp
  ) AS experience,

  -- Education array (parse from JSON)
  ARRAY(
    SELECT AS STRUCT
      TO_HEX(MD5(CONCAT(
        COALESCE(JSON_VALUE(edu, '$.title'), ''),
        COALESCE(JSON_VALUE(edu, '$.degree'), ''),
        ''
      ))) AS education_id,
      JSON_VALUE(edu, '$.title') AS institution_name,
      JSON_VALUE(edu, '$.degree') AS degree,
      JSON_VALUE(edu, '$.field') AS field_of_study,
      CAST(NULL AS DATE) AS start_date,
      CAST(NULL AS DATE) AS end_date,
      'source_2' AS source_system
    FROM UNNEST(JSON_QUERY_ARRAY(education_json)) AS edu
  ) AS education,

  -- Certifications (Source 2 exclusive, parse from JSON)
  ARRAY(
    SELECT AS STRUCT
      JSON_VALUE(cert, '$.title') AS title,
      JSON_VALUE(cert, '$.subtitle') AS issuing_org,
      CAST(NULL AS DATE) AS issue_date,
      JSON_VALUE(cert, '$.credential_id') AS credential_id
    FROM UNNEST(JSON_QUERY_ARRAY(certifications_json)) AS cert
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
