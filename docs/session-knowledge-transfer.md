# Session Knowledge Transfer

This document captures key decisions, context, and learnings from the development session.

## Project Overview

**Assignment**: Data Engineering Take-Home (4-5 hours)
**GCP Project**: `coffeespace-sandbox`
**Location**: `us-central1`

### Priority Order (from assignment)
1. **Critical**: Part 4 (BQ → Firestore Sync), Part 3 (Merge Logic)
2. **Important**: Part 1 (Profiling), Part 2 (Schema)
3. **Writeup**: Part 5 (Design Questions)
4. **Required**: Part 6 (README)

---

## Completed Parts

### Part 1: Data Profiling
- **Deliverables**: `docs/part-1-data-profiling/`
- **Key findings**:
  - Source 1 (Aviato): camelCase, ISO8601 dates, nested with company objects
  - Source 2 (LinkedIn Scraper): snake_case, "Oct 2024" dates, flatter structure
  - Join key: `linkedin_num_id` (INT64 in S1, STRING in S2)
  - Source 1 more reliable for: names (99.6%), about (84.5%), dates, location hierarchy
  - Source 2 exclusive: certifications (16%), activity/posts

### Part 2: Canonical Schema
- **Deliverables**: `docs/part-2-canonical-schema/`
- **Architecture**: 2-layer model
  - `people_canonical`: Full provenance, nested STRUCTs
  - `firestore_export`: Lean view with `sync_hash` for delta detection
- **Key schema elements**:
  - `identity_sources` array: Never lose provenance data
  - `normalization_errors` array: Track dirty fields explicitly
  - `sync_hash`: MD5 of synced fields for O(1) change detection

### Part 3: Merge Logic (In Progress)
- **Deliverables**: `scripts/sql/*.sql`, `scripts/part3_pipeline.py`, `docs/part-3-merge-logic/`
- **Architecture**: Bronze → Silver → Gold
  - Bronze: BQ external tables on GCS (`raw_source_1`, `raw_source_2`)
  - Silver: Staging tables with canonical types (`stg_source_1`, `stg_source_2`)
  - Gold: Merged canonical (`people_canonical`)

---

## Key Technical Decisions

### External Tables
```sql
-- Source 1 (JSONL)
format = 'NEWLINE_DELIMITED_JSON'
autodetect = true
max_bad_records = 100

-- Source 2 (JSON files)
format = 'JSON'
uris = ['gs://coffeespace-sandbox-source-2/*.json']
```

### Merge Strategy
- **FULL OUTER JOIN** on `linkedin_num_id`
  - Handles A-only, B-only, A+B naturally
  - Assignment says sources are "mostly distinct"
- **Field resolution**: Prefer Source 1 for identity (higher completeness)
- **Arrays**: Union + dedupe by deterministic IDs

### Deterministic IDs
```sql
-- Experience ID
TO_HEX(MD5(CONCAT(company_id, title, start_date)))

-- Education ID
TO_HEX(MD5(CONCAT(institution, degree, start_date)))
```

### Derived Fields (Assignment Requirement)
- `primary_portfolio`: Categorize by headline keywords
- `years_of_experience`: Sum of experience durations

---

## Data Sources

| Source | Location | Format | Size |
|--------|----------|--------|------|
| Source 1 | `gs://coffeespace-sandbox-source-1/CoffeeSpaceTestDatav4.jsonl` | JSONL | ~11GB |
| Source 2 | `gs://coffeespace-sandbox-source-2/*.json` | JSON | 863 files |

---

## BigQuery Dataset

- **Dataset**: `coffeespace_canonical`
- **Location**: `us-central1`
- **Tables**:
  - `raw_source_1` (external)
  - `raw_source_2` (external)
  - `stg_source_1`
  - `stg_source_2`
  - `people_canonical`
- **Views**:
  - `firestore_export`

---

## File Structure

```
scripts/
├── part3_pipeline.py          # Orchestration
├── profile_sources.py         # Part 1 profiling
└── sql/
    ├── 01_create_external_tables.sql
    ├── 02_validate_sources.sql
    ├── 03_staging_source_1.sql
    ├── 04_staging_source_2.sql
    ├── 05_merge_canonical.sql
    ├── 06_derived_fields.sql
    └── 07_firestore_export_view.sql

docs/
├── part-1-data-profiling/
├── part-2-canonical-schema/
├── part-3-merge-logic/
│   ├── dedup-rules.md
│   └── dirty-field-handling.md
└── decisions/
    └── decision-log.md
```

---

## Dependencies (pyproject.toml)

```toml
dependencies = [
    "google-cloud-bigquery>=3.0.0",
    "google-cloud-firestore>=2.0.0",
    "google-cloud-storage>=2.0.0",
    "db-dtypes>=1.0.0",
    "pandas>=3.0.0",
    "numpy>=2.4.2",
    "ipykernel>=7.1.0",
]
```

---

## Remaining Work

### Part 3 (Complete pipeline run)
- Run `uv run python scripts/part3_pipeline.py`
- Fix any SQL errors from actual data structure
- Verify row counts and deduplication

### Part 4: BQ → Firestore Sync
- Create Firestore database
- Implement incremental sync using `sync_hash`
- Batch writes (500 docs/batch)
- Performance report required

### Part 5: Design Questions (Writeup only)
1. Snapshot versioning, delta detection, rollback
2. Validation/DQ checks before serving
3. Handling 30% fewer records scenario
4. Merge logic for newer data with null fields
5. Adding third data source
6. Scaling to tens of millions

### Part 6: README
- Required for submission
- Non-engineer audience

---

## Key Learnings / Gotchas

1. **BQ JSON format**: Use `NEWLINE_DELIMITED_JSON` for JSONL files
2. **Schema autodetect**: Risky with heterogeneous files, add validation
3. **Source 2 dates**: "Oct 2024" format needs `PARSE_DATE('%b %Y', ...)`
4. **Source 2 linkedin_num_id**: Stored as STRING, needs `SAFE_CAST`
5. **Firestore location**: Must match BQ dataset region (us-central1)
6. **External tables**: Zero-copy but schema inference can be brittle

---

## Commands Reference

```bash
# Authenticate
gcloud auth login
gcloud config set project coffeespace-sandbox
gcloud auth application-default login

# BigQuery
bq mk --location=us-central1 coffeespace_canonical
bq ls

# Firestore
gcloud firestore databases create --location=nam5 --type=firestore-native

# Run pipeline
uv run python scripts/part3_pipeline.py
```
