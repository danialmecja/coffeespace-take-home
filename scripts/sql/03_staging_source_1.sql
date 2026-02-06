-- Step 3: Staging Source 1 (Silver Layer)
-- Normalizes to canonical schema with error tracking
-- Join key: linkedinID (URL slug like "john-doe-123")

CREATE OR REPLACE TABLE `coffeespace-sandbox.coffeespace_canonical.stg_source_1` AS

WITH parsed AS (
  SELECT
    *,
    ARRAY<STRUCT<field STRING, error STRING, raw_value STRING>>[] AS _errors
  FROM `coffeespace-sandbox.coffeespace_canonical.raw_source_1`
),

with_errors AS (
  SELECT
    p.*,
    ARRAY_CONCAT(
      p._errors,
      IF(linkedinID IS NULL,
         [STRUCT('linkedin_id' AS field, 'NULL_VALUE' AS error, '' AS raw_value)],
         []),
      IF(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', lastUpdated) IS NULL AND lastUpdated IS NOT NULL,
         [STRUCT('last_updated' AS field, 'INVALID_TIMESTAMP' AS error, lastUpdated AS raw_value)],
         [])
    ) AS normalization_errors
  FROM parsed p
)

SELECT
  -- Primary key: URL slug (e.g., "john-doe-123")
  linkedinID AS linkedin_id,

  -- Identity (canonical struct)
  STRUCT(
    fullName AS full_name,
    firstName AS first_name,
    lastName AS last_name,
    headline,
    about
  ) AS identity,

  -- Identity sources (for provenance tracking)
  [STRUCT(
    'source_1' AS source_system,
    fullName AS full_name,
    headline,
    about,
    SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', lastUpdated) AS last_updated
  )] AS identity_sources,

  -- Location (canonical struct with hierarchy)
  STRUCT(
    location AS display_string,
    JSON_VALUE(TO_JSON_STRING(locationDetails), '$.country.name') AS country,
    JSON_VALUE(TO_JSON_STRING(locationDetails), '$.region.name') AS region,
    JSON_VALUE(TO_JSON_STRING(locationDetails), '$.locality.name') AS locality,
    CAST(NULL AS STRING) AS country_code,
    locationIDList AS location_ids
  ) AS location,

  -- Social metrics
  STRUCT(
    linkedinConnections AS connections,
    linkedinFollowers AS followers,
    SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', lastUpdated) AS metrics_as_of
  ) AS social_metrics,

  -- Experience array with deterministic IDs
  ARRAY(
    SELECT AS STRUCT
      TO_HEX(MD5(CONCAT(
        COALESCE(exp.companyID, ''),
        COALESCE(pos.title, ''),
        COALESCE(pos.startDate, '')
      ))) AS experience_id,
      exp.companyName AS company_name,
      exp.companyID AS company_linkedin_id,
      pos.title AS title,
      SAFE.PARSE_DATE('%Y-%m-%d', SUBSTR(pos.startDate, 1, 10)) AS start_date,
      SAFE.PARSE_DATE('%Y-%m-%d', SUBSTR(pos.endDate, 1, 10)) AS end_date,
      pos.location AS location,
      pos.description AS description,
      (pos.endDate IS NULL) AS is_current,
      'source_1' AS source_system
    FROM UNNEST(experienceList) AS exp,
    UNNEST(exp.positionList) AS pos
  ) AS experience,

  -- Education array with deterministic IDs
  ARRAY(
    SELECT AS STRUCT
      TO_HEX(MD5(CONCAT(
        COALESCE(edu.schoolName, ''),
        COALESCE(edu.degree, ''),
        COALESCE(edu.startDate, '')
      ))) AS education_id,
      edu.schoolName AS institution_name,
      edu.degree,
      edu.fieldOfStudy AS field_of_study,
      SAFE.PARSE_DATE('%Y-%m-%d', SUBSTR(edu.startDate, 1, 10)) AS start_date,
      SAFE.PARSE_DATE('%Y-%m-%d', SUBSTR(edu.endDate, 1, 10)) AS end_date,
      'source_1' AS source_system
    FROM UNNEST(educationList) AS edu
  ) AS education,

  -- Certifications (empty for Source 1)
  CAST([] AS ARRAY<STRUCT<title STRING, issuing_org STRING, issue_date DATE, credential_id STRING>>) AS certifications,

  -- Skills
  skills,

  -- Computed signals (Source 1 exclusive)
  STRUCT(
    computed_likelyToExplore AS likely_to_explore,
    computed_recentlyLeftCompany AS recently_left_company,
    computed_potentialToLeave AS potential_to_leave,
    computed_priorBackedFounder AS prior_backed_founder,
    computed_unicornEarlyEngineer AS unicorn_early_engineer,
    computed_bigTechAlumPrivate AS big_tech_alum_private,
    computed_bigTechAlumPublic AS big_tech_alum_public
  ) AS computed_signals,

  -- Provenance
  id AS source_id,
  'source_1' AS source_system,
  SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', lastUpdated) AS last_updated,

  -- Error tracking
  normalization_errors

FROM with_errors
WHERE linkedinID IS NOT NULL;
