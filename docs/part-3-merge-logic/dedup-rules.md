# Deduplication & Merge Rules

This document describes the deterministic rules for merging Source 1 (Aviato) and Source 2 (LinkedIn Scraper) data.

## Join Strategy

**FULL OUTER JOIN on `linkedin_num_id`**

This naturally handles three cases:
1. **A-only**: Records in Source 1 but not Source 2 → `s2.*` columns are NULL
2. **B-only**: Records in Source 2 but not Source 1 → `s1.*` columns are NULL
3. **A+B**: Records in both sources → merge using rules below

## Field Resolution Rules

| Field | Resolution Rule | Rationale |
|-------|-----------------|-----------|
| `identity.full_name` | `COALESCE(s1, s2)` | Source 1 has 99.6% completeness vs 99.2% |
| `identity.first_name` | `COALESCE(s1, s2)` | Source 1 more complete |
| `identity.last_name` | `COALESCE(s1, s2)` | Source 1 more complete |
| `identity.headline` | `COALESCE(s1, s2)` | Source 1 has 99.8% completeness |
| `identity.about` | `COALESCE(s1, s2)` | Source 1 has 84.5% vs 40.5% |
| `identity_sources` | `ARRAY_CONCAT(s1, s2)` | **Never lose data** — keep both |
| `location.*` | Prefer Source 1 | Has hierarchical structure + Who's On First IDs |
| `social_metrics.connections` | Most recent `metrics_as_of` | Time-sensitive metric |
| `social_metrics.followers` | Most recent `metrics_as_of` | Time-sensitive metric |
| `experience` | Union + dedupe by `experience_id` | More coverage from both sources |
| `education` | Union + dedupe by `education_id` | More coverage from both sources |
| `certifications` | Source 2 only | Exclusive to Source 2 |
| `skills` | `ARRAY_AGG(DISTINCT ...)` | Union and dedupe |
| `computed_signals` | Source 1 only | Exclusive to Source 1 (Aviato enrichment) |

## Deterministic ID Generation

Experience and education records are deduplicated using deterministic IDs:

```sql
-- Experience ID
TO_HEX(MD5(CONCAT(
  COALESCE(company_linkedin_id, company_name, ''),
  COALESCE(title, ''),
  COALESCE(CAST(start_date AS STRING), '')
)))

-- Education ID
TO_HEX(MD5(CONCAT(
  COALESCE(institution_name, ''),
  COALESCE(degree, ''),
  COALESCE(CAST(start_date AS STRING), '')
)))
```

This ensures the same position/degree from both sources collapses to one record.

## Provenance Tracking

Every canonical record tracks its source lineage:

```sql
provenance.source_systems = ['source_1']           -- A-only
provenance.source_systems = ['source_2']           -- B-only
provenance.source_systems = ['source_1', 'source_2'] -- A+B (merged)
```

Additionally:
- `source_1_id`: Original ID from Source 1
- `source_2_id`: Original ID from Source 2
- `source_1_last_updated`: When Source 1 record was last updated
- `source_2_last_updated`: When Source 2 record was last updated

## Idempotency

The pipeline is idempotent because:
1. All tables use `CREATE OR REPLACE`
2. Join key (`linkedin_num_id`) is deterministic
3. `canonical_id` is a deterministic hash of `linkedin_num_id`
4. Experience/education IDs are deterministic hashes

Running the pipeline twice produces identical results.
