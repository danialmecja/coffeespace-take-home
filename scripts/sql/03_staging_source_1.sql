-- Step 3: Staging Source 1 (Silver Layer)
-- Normalizes to canonical schema with error tracking
-- Join key: linkedinID (URL slug like "john-doe-123")
--
-- NOTE: lastUpdated is already a TIMESTAMP (BigQuery autodetected it)

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
      IF(lastUpdated IS NULL,
         [STRUCT('last_updated' AS field, 'NULL_VALUE' AS error, '' AS raw_value)],
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
    lastUpdated AS last_updated  -- Already TIMESTAMP
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
    lastUpdated AS metrics_as_of  -- Already TIMESTAMP
  ) AS social_metrics,

  -- Experience array with deterministic IDs
  -- Note: startDate/endDate may be TIMESTAMP (autodetected), cast to STRING for hashing
  ARRAY(
    SELECT AS STRUCT
      TO_HEX(MD5(CONCAT(
        COALESCE(exp.companyID, ''),
        COALESCE(pos.title, ''),
        COALESCE(CAST(pos.startDate AS STRING), '')
      ))) AS experience_id,
      exp.companyName AS company_name,
      exp.companyID AS company_linkedin_id,
      pos.title AS title,
      SAFE_CAST(pos.startDate AS DATE) AS start_date,
      SAFE_CAST(pos.endDate AS DATE) AS end_date,
      pos.location AS location,
      pos.description AS description,
      (pos.endDate IS NULL) AS is_current,
      'source_1' AS source_system
    FROM UNNEST(experienceList) AS exp,
    UNNEST(exp.positionList) AS pos
  ) AS experience,

  -- Education array with deterministic IDs
  -- Note: Source 1 uses 'name' (not schoolName), 'subject' (not fieldOfStudy), no 'degree' field
  ARRAY(
    SELECT AS STRUCT
      TO_HEX(MD5(CONCAT(
        COALESCE(edu.name, ''),
        COALESCE(edu.subject, ''),
        COALESCE(CAST(edu.startDate AS STRING), '')
      ))) AS education_id,
      edu.name AS institution_name,
      CAST(NULL AS STRING) AS degree,  -- Not in Source 1
      edu.subject AS field_of_study,
      SAFE_CAST(edu.startDate AS DATE) AS start_date,
      SAFE_CAST(edu.endDate AS DATE) AS end_date,
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
  lastUpdated AS last_updated,  -- Already TIMESTAMP

  -- Error tracking
  normalization_errors

FROM with_errors
WHERE linkedinID IS NOT NULL;
