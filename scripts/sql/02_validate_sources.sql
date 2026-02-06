-- Step 2: Validate Sources Before Staging
-- Check for schema drift and critical field issues

-- Source 1: Validate critical fields
-- Note: Join key is linkedinID (URL slug), not linkedinNumID
SELECT
  'source_1' AS source,
  COUNT(*) AS total_rows,
  COUNTIF(linkedinID IS NULL) AS missing_linkedin_id,
  COUNTIF(fullName IS NULL) AS missing_full_name,
  COUNTIF(lastUpdated IS NULL) AS missing_last_updated,
  COUNTIF(experienceList IS NULL) AS missing_experience
FROM `coffeespace-sandbox.coffeespace_canonical.raw_source_1`;

-- Source 2: Validate critical fields (parse from json_line column)
-- Note: Join key is linkedin_id (matches Source 1's linkedinID)
SELECT
  'source_2' AS source,
  COUNT(*) AS total_rows,
  COUNTIF(JSON_VALUE(json_line, '$.linkedin_id') IS NULL) AS missing_linkedin_id,
  COUNTIF(JSON_VALUE(json_line, '$.name') IS NULL) AS missing_name,
  COUNTIF(JSON_QUERY(json_line, '$.experience') IS NULL) AS missing_experience
FROM `coffeespace-sandbox.coffeespace_canonical.raw_source_2_sample50`;
