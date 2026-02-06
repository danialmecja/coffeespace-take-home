# CoffeeSpace Data Engineering Pipeline

A data engineering pipeline that ingests, normalizes, and merges LinkedIn profile data from two heterogeneous sources into a unified canonical schema in BigQuery.

## Problem Statement

CoffeeSpace has two data sources containing LinkedIn profile information:

| Source | Format | Size | Records |
|--------|--------|------|---------|
| Source 1 (Aviato) | JSONL | ~11GB | ~500K profiles |
| Source 2 (Scraper) | JSON arrays | ~35GB (863 files) | ~800K profiles |

These sources have different schemas, naming conventions, date formats, and data quality characteristics. The goal is to merge them into a single source of truth that can be queried in BigQuery and synced to Firestore.

## Architecture

The pipeline follows a medallion architecture adapted for GCP:

```
┌─────────────────┐     ┌─────────────────┐
│  GCS: Source 1  │     │  GCS: Source 2  │
│  (JSONL)        │     │  (JSON arrays)  │
└────────┬────────┘     └────────┬────────┘
         │                       │
         ▼                       ▼
┌─────────────────┐     ┌─────────────────┐
│  raw_source_1   │     │  raw_source_2   │   ← Bronze (raw ingestion)
│  (external tbl) │     │  (loaded table) │
└────────┬────────┘     └────────┬────────┘
         │                       │
         ▼                       ▼
┌─────────────────┐     ┌─────────────────┐
│  stg_source_1   │     │  stg_source_2   │   ← Silver (normalized)
│  canonical types│     │  canonical types│
└────────┬────────┘     └────────┬────────┘
         │                       │
         └───────────┬───────────┘
                     ▼
           ┌─────────────────┐
           │ people_canonical│                 ← Gold (merged)
           │ FULL OUTER JOIN │
           └────────┬────────┘
                    │
                    ▼
           ┌─────────────────┐
           │ firestore_export│                 ← Serving view
           │ (flattened)     │
           └─────────────────┘
```

## Project Structure

```
├── docs/
│   ├── part-1-data-profiling/
│   │   ├── data-profiling-report.md    # Data quality findings
│   │   └── key-discovery.ipynb         # Join key analysis
│   ├── part-2-canonical-schema/
│   │   ├── schema.sql                  # BigQuery DDL
│   │   ├── schema.json                 # JSON Schema
│   │   ├── field-rationale.md          # Field-by-field justification
│   │   └── example-merged-record.json  # Sample merged record
│   ├── part-3-merge-logic/
│   │   ├── dedup-rules.md              # Deduplication strategy
│   │   └── dirty-field-handling.md     # Error tracking approach
│   └── decisions/
│       └── decision-log.md             # Key design decisions
├── scripts/
│   ├── profile_sources.py              # Data profiling script
│   ├── load_source_2.sh                # Source 2 loader (Cloud Shell)
│   ├── load_source_2_streaming.py      # Alternative parallel loader
│   ├── part3_pipeline.py               # Pipeline orchestration
│   └── sql/
│       ├── 01_create_external_tables.sql
│       ├── 02_validate_sources.sql
│       ├── 03_staging_source_1.sql
│       ├── 04_staging_source_2.sql
│       ├── 05_merge_canonical.sql
│       ├── 06_derived_fields.sql
│       └── 07_firestore_export_view.sql
└── pyproject.toml                      # Python dependencies
```

## Key Design Decisions

### Join Key

The pipeline uses `linkedin_id` (the URL slug, e.g., `john-doe-123`) as the join key between sources. This was chosen over `linkedin_num_id` because:
- Present and non-null in both sources
- Stable across time (URL slugs rarely change)
- Human-readable for debugging

### Merge Strategy

A FULL OUTER JOIN handles three cases:
- Records only in Source 1
- Records only in Source 2
- Records in both sources (merged with field-level resolution rules)

### Field Resolution Rules

| Field | Resolution | Rationale |
|-------|------------|-----------|
| Identity (name, headline, about) | Prefer Source 1 | Higher completeness (99.6% vs 99.2%) |
| Location | Prefer Source 1 | Has hierarchical structure + location IDs |
| Social metrics | Most recent | Time-sensitive data |
| Experience/Education | Union both | Maximizes coverage |
| Certifications | Source 2 only | Exclusive to that source |
| Computed signals | Source 1 only | Exclusive Aviato enrichment |

### Provenance Tracking

Every merged record tracks:
- Which sources contributed (`source_systems` array)
- Original source IDs (`source_1_id`, `source_2_id`)
- Last update timestamps from each source
- Normalization errors encountered during ETL

## Running the Pipeline

### Prerequisites

- GCP project with BigQuery and GCS access
- Python 3.11+
- `gcloud` CLI authenticated

### Setup

```bash
# Install dependencies
uv sync

# Authenticate to GCP
gcloud auth application-default login
```

### Step 1: Load Source 2 (from Cloud Shell)

Source 2 files are JSON arrays, not JSONL. Load them via Cloud Shell for best performance:

```bash
# Convert JSON arrays to NDJSON and upload to staging bucket
gsutil cat 'gs://coffeespace-sandbox-source-2/*.json' \
  | jq -c 'if type=="array" then .[] else . end' \
  | gsutil cp - 'gs://coffeespace-sandbox-source-2-ndjson/source2.ndjson'

# Load into BigQuery
bq load \
  --source_format=NEWLINE_DELIMITED_JSON \
  --autodetect \
  --replace \
  coffeespace_canonical.raw_source_2 \
  'gs://coffeespace-sandbox-source-2-ndjson/source2.ndjson'
```

### Step 2: Run the Pipeline

```bash
uv run python scripts/part3_pipeline.py
```

This executes:
1. Creates external table for Source 1
2. Validates both sources
3. Stages Source 1 to canonical schema
4. Stages Source 2 to canonical schema
5. Merges to `people_canonical`
6. Computes derived fields
7. Creates Firestore export view

### Verification

```sql
-- Check row counts
SELECT 'raw_source_1' as tbl, COUNT(*) as cnt FROM `coffeespace_canonical.raw_source_1`
UNION ALL SELECT 'raw_source_2', COUNT(*) FROM `coffeespace_canonical.raw_source_2`
UNION ALL SELECT 'people_canonical', COUNT(*) FROM `coffeespace_canonical.people_canonical`;

-- Check provenance distribution
SELECT ARRAY_TO_STRING(provenance.source_systems, ',') as sources, COUNT(*)
FROM `coffeespace_canonical.people_canonical`
GROUP BY 1;

-- Verify no duplicates
SELECT linkedin_id, COUNT(*) as cnt
FROM `coffeespace_canonical.people_canonical`
GROUP BY 1 HAVING cnt > 1;
```

## Data Quality

### Source Comparison

| Metric | Source 1 | Source 2 |
|--------|----------|----------|
| Name completeness | 99.6% | 99.2% |
| About completeness | 84.5% | 40.5% |
| Date format | ISO8601 | Human-readable ("Oct 2024") |
| Location structure | Hierarchical | Flat strings |

### Normalization Errors

The pipeline tracks data quality issues without dropping records:

```sql
SELECT linkedin_id, normalization_errors
FROM `coffeespace_canonical.people_canonical`
WHERE ARRAY_LENGTH(normalization_errors) > 0;
```

Common issues tracked:
- `NULL_VALUE`: Required field missing
- `INVALID_TIMESTAMP`: Unparseable date
- `EXTRA_WHITESPACE`: Name has multiple spaces

## Derived Fields

The pipeline computes two derived fields:

1. **`primary_portfolio`**: Job function category derived from headline keywords
   - Software Engineering, Data Science, Product Management, Design, Sales, Marketing, Finance, HR, Operations, Executive, Other

2. **`years_of_experience`**: Total career duration calculated from experience dates

## Dependencies

- `google-cloud-bigquery` - BigQuery client
- `google-cloud-storage` - GCS client
- `google-cloud-firestore` - Firestore client (for Part 4)
- `db-dtypes` - BigQuery data type support
