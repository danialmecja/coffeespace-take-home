-- Step 7: Firestore Export View
-- Lean projection with sync_hash for Part 4 delta detection

CREATE OR REPLACE VIEW `coffeespace-sandbox.coffeespace_canonical.firestore_export` AS
SELECT
  -- Document ID (URL slug)
  linkedin_id AS doc_id,
  linkedin_id,

  -- Flat identity fields
  identity.full_name,
  identity.first_name,
  identity.last_name,
  identity.headline,
  identity.about,

  -- Flat location fields
  location.display_string AS location_display,
  location.country AS location_country,
  location.country_code AS location_country_code,

  -- Social metrics
  social_metrics.connections,
  social_metrics.followers,

  -- Derived fields (assignment requirement)
  derived_fields.primary_portfolio,
  derived_fields.years_of_experience,

  -- Arrays as JSON for Firestore
  TO_JSON_STRING(experience) AS experience_json,
  TO_JSON_STRING(education) AS education_json,
  TO_JSON_STRING(certifications) AS certifications_json,
  skills,

  -- Computed signals (flat for filtering)
  computed_signals.likely_to_explore AS computed_likely_to_explore,
  computed_signals.potential_to_leave AS computed_potential_to_leave,

  -- Provenance
  provenance.source_systems,

  -- Delta detection: last modification timestamp
  GREATEST(
    COALESCE(provenance.source_1_last_updated, TIMESTAMP('1970-01-01')),
    COALESCE(provenance.source_2_last_updated, TIMESTAMP('1970-01-01')),
    provenance.last_merged_at
  ) AS last_modified_at,

  -- Sync hash for O(1) change detection in Part 4
  TO_HEX(MD5(CONCAT(
    COALESCE(identity.full_name, ''),
    COALESCE(identity.headline, ''),
    COALESCE(identity.about, ''),
    COALESCE(location.display_string, ''),
    CAST(COALESCE(social_metrics.connections, 0) AS STRING),
    CAST(COALESCE(social_metrics.followers, 0) AS STRING),
    COALESCE(derived_fields.primary_portfolio, ''),
    CAST(COALESCE(derived_fields.years_of_experience, 0) AS STRING),
    TO_JSON_STRING(experience),
    TO_JSON_STRING(education),
    TO_JSON_STRING(certifications),
    ARRAY_TO_STRING(COALESCE(skills, []), ',')
  ))) AS sync_hash

FROM `coffeespace-sandbox.coffeespace_canonical.people_canonical`;
