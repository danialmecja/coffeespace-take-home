-- Canonical Schema: people_canonical
-- Grain: One row per real-world person (identified by linkedin_num_id)
-- Architecture: 2-layer model (canonical for correctness, export view for sync)

CREATE TABLE IF NOT EXISTS `project.dataset.people_canonical` (
  -- Primary identifier
  canonical_id STRING NOT NULL OPTIONS(description="Deterministic hash of linkedin_num_id"),
  linkedin_num_id INT64 NOT NULL OPTIONS(description="Natural key from LinkedIn, used for cross-source joins"),

  -- Resolved identity (merged values from all sources)
  identity STRUCT<
    full_name STRING,
    first_name STRING,
    last_name STRING,
    headline STRING,
    about STRING
  > OPTIONS(description="Canonical identity fields after conflict resolution"),

  -- Source-specific identity values (provenance - never lose data)
  identity_sources ARRAY<STRUCT<
    source_system STRING,
    full_name STRING,
    headline STRING,
    about STRING,
    last_updated TIMESTAMP
  >> OPTIONS(description="Per-source identity values for audit and debugging"),

  -- Location (canonical)
  location STRUCT<
    display_string STRING,
    country STRING,
    region STRING,
    locality STRING,
    country_code STRING,
    location_ids ARRAY<INT64>
  > OPTIONS(description="Hierarchical location from Source 1, or parsed from Source 2"),

  -- Social metrics
  social_metrics STRUCT<
    connections INT64,
    followers INT64,
    metrics_as_of TIMESTAMP
  > OPTIONS(description="Time-sensitive metrics with freshness tracking"),

  -- Experience array (union from both sources, deduped)
  experience ARRAY<STRUCT<
    experience_id STRING,
    company_name STRING,
    company_linkedin_id STRING,
    title STRING,
    start_date DATE,
    end_date DATE,
    location STRING,
    description STRING,
    is_current BOOL,
    source_system STRING
  >> OPTIONS(description="Work history, deduped by company+title+dates"),

  -- Education array
  education ARRAY<STRUCT<
    education_id STRING,
    institution_name STRING,
    degree STRING,
    field_of_study STRING,
    start_date DATE,
    end_date DATE,
    source_system STRING
  >> OPTIONS(description="Education history, deduped by institution+degree+dates"),

  -- Certifications (Source 2 exclusive)
  certifications ARRAY<STRUCT<
    title STRING,
    issuing_org STRING,
    issue_date DATE,
    credential_id STRING
  >> OPTIONS(description="Professional certifications from Source 2"),

  -- Skills
  skills ARRAY<STRING> OPTIONS(description="Combined skill set from all sources"),

  -- Computed signals (Source 1 exclusive - preserve valuable enrichment)
  computed_signals STRUCT<
    likely_to_explore BOOL,
    recently_left_company BOOL,
    potential_to_leave BOOL,
    prior_backed_founder BOOL,
    unicorn_early_engineer BOOL,
    big_tech_alum_private BOOL,
    big_tech_alum_public BOOL
  > OPTIONS(description="Aviato-computed talent signals from Source 1"),

  -- Derived fields (assignment requirement)
  derived_fields STRUCT<
    primary_portfolio STRING,
    years_of_experience FLOAT64,
    computation_method STRING
  > OPTIONS(description="Computed during merge: portfolio category and tenure"),

  -- Provenance tracking
  provenance STRUCT<
    source_systems ARRAY<STRING>,
    source_1_id STRING,
    source_2_id STRING,
    source_1_last_updated TIMESTAMP,
    source_2_last_updated TIMESTAMP,
    first_seen_at TIMESTAMP,
    last_merged_at TIMESTAMP,
    record_version INT64
  > OPTIONS(description="Full source lineage for auditing"),

  -- Sync metadata (for Part 4 Firestore sync)
  sync_metadata STRUCT<
    firestore_doc_id STRING,
    last_synced_at TIMESTAMP,
    sync_hash STRING
  > OPTIONS(description="Change detection for incremental Firestore sync")
)
PARTITION BY DATE(provenance.last_merged_at)
CLUSTER BY linkedin_num_id
OPTIONS(
  description="Canonical person records. Grain: one row per real-world person."
);

-- Index for common lookups
-- Note: BQ doesn't have traditional indexes, but clustering helps

--------------------------------------------------------------------------------
-- Layer 2: Firestore Export View
-- Flattened for efficient sync with delta detection
--------------------------------------------------------------------------------

CREATE OR REPLACE VIEW `project.dataset.firestore_export` AS
SELECT
  -- Document ID (deterministic)
  FORMAT('%d', linkedin_num_id) AS doc_id,
  linkedin_num_id,

  -- Identity (flattened)
  identity.full_name,
  identity.first_name,
  identity.last_name,
  identity.headline,
  identity.about,

  -- Location (flattened)
  location.display_string AS location_display,
  location.country AS location_country,
  location.country_code AS location_country_code,

  -- Social metrics
  social_metrics.connections,
  social_metrics.followers,

  -- Derived fields
  derived_fields.primary_portfolio,
  derived_fields.years_of_experience,

  -- Arrays as JSON for Firestore native parsing
  TO_JSON_STRING(experience) AS experience_json,
  TO_JSON_STRING(education) AS education_json,
  TO_JSON_STRING(certifications) AS certifications_json,
  skills,

  -- Computed signals (flattened for filtering)
  computed_signals.likely_to_explore AS computed_likely_to_explore,
  computed_signals.potential_to_leave AS computed_potential_to_leave,

  -- Provenance (minimal for serving)
  provenance.source_systems,

  -- Delta detection
  GREATEST(
    COALESCE(provenance.source_1_last_updated, TIMESTAMP('1970-01-01')),
    COALESCE(provenance.source_2_last_updated, TIMESTAMP('1970-01-01')),
    provenance.last_merged_at
  ) AS last_modified_at,

  -- Sync hash: hash of all synced fields for O(1) change detection
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
    ARRAY_TO_STRING(skills, ',')
  ))) AS sync_hash

FROM `project.dataset.people_canonical`;
