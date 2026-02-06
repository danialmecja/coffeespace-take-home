## Part 1
Usually for DQ checks, first: i check samples to look at schema structure, naming convs, data model differences etc.
Then, check out these:
- Row count: For scale of data
- Null rate per field: For completeness
- Distinct val count: for cardinality
- Min/max/avg: distribution
- Pattern freq.: Format consistency
- Type violations: reliability of the given schema
- dupes: Uniqueness

To do this I'll:
- run profiling queries from 
  - streaming
  - or BQ
- Then generate stat. evidence for DQ claims

Given profiling: Source 1 is more reliable for IDs, dates, location hierarchy, company enrichment, and structured experience, while Source 2 adds certifications, activity/posts, and richer images.

**Deliverables:**
- `docs/part-1-data-profiling/data-profiling-report.md` - Full profiling report with completeness metrics
- `docs/part-1-data-profiling/profiles-raw.json` - Raw profiling data
- `docs/part-1-data-profiling/key-discovery.ipynb` - Key intersection analysis

---

## Part 2: Canonical Schema Design

**Decision: 2-Layer Architecture**

| Layer | Table/View | Purpose |
|-------|------------|---------|
| Canonical | `people_canonical` | Source of truth with nested STRUCTs, full provenance |
| Export | `firestore_export` (view) | Flattened projection for Firestore sync with `sync_hash` |

**Rationale:** The CRITICAL priority is fixing BQ → Firestore sync. Sync performance depends on delta detection + batching, not schema structure. Separating layers lets us:
- Use nested STRUCTs for modeling correctness (provenance, no silent data loss)
- Use flat projections with `sync_hash` for O(1) change detection

**Key Schema Decisions:**

| Decision | Choice | Evidence |
|----------|--------|----------|
| Primary key | `linkedin_num_id` (INT64) | Present in both sources, immutable (vs URL slugs) |
| Identity resolution | Prefer Source 1 | Higher completeness (99.6% vs 99.2% for names, 84.5% vs 40.5% for about) |
| Provenance | `identity_sources` array | Never silently lose data when sources conflict |
| Experience/Education | Union + dedupe | More coverage, dedupe by company+title+dates |
| Certifications | Source 2 only | Exclusive to Source 2 (16% have them) |
| Computed signals | Source 1 only | Exclusive Aviato enrichment (84.7% populated) |
| Derived fields | Compute at merge time | `primary_portfolio` from titles, `years_of_experience` from date math |
| Change detection | `sync_hash` column | MD5 of all synced fields → O(1) delta detection for Part 4 |

**Deliverables:**
- `docs/part-2-canonical-schema/schema.sql` - BigQuery DDL
- `docs/part-2-canonical-schema/schema.json` - JSON Schema for documentation
- `docs/part-2-canonical-schema/field-rationale.md` - Field-by-field justification
- `docs/part-2-canonical-schema/example-merged-record.json` - Concrete merge example

Yep — overall the plan is **directionally solid** (SQL-first in BQ, bronze/silver/gold layering, deterministic dedupe on `linkedin_num_id`, explicit provenance), and it matches what the assignment cares about most (Part 3 merge logic + Part 4 sync). 

But there are a few **technical landmines** in that writeup that will bite you in the sandbox unless you tighten them. I’ll go point-by-point: what’s valid, what’s risky/wrong, and what I’d change.

---

## What’s good / aligns with best practice

### ✅ SQL-first in BigQuery (with Python only orchestrating)

This is a very normal “production” split: SQL is declarative + auditable; orchestration in Python/Airflow/dbt/etc. is common. (Your assignment explicitly wants “real DE work” + idempotent merge logic .)

### ✅ Bronze → Silver → Gold layout

This matches the “raw retained / normalized / canonical” pattern that avoids over-cleaning or silent mutation, and makes backfills/debugging feasible. 

### ✅ Deterministic dedupe key: `linkedin_num_id`

This is explicitly recommended by your own profiling: normalize Source 2 from string to int and use it as the join key. 

---

## What’s risky / incorrect in the plan (and how to fix)

### 1) External tables for JSON/JSONL: the DDL is incomplete

They wrote external table DDL without **autodetect** or schema. For JSON external tables, schema detection is something you must enable (or define schema). ([Google Cloud Documentation][1]) ([Google Cloud Documentation][2])

**Fix**
Add `OPTIONS (format='JSON', uris=[...], autodetect=true)` (or explicitly define schema). If you don’t, you can get failures or weird partial schemas.

Also: Source A is JSONL; you want to confirm the correct external-table format setting for newline-delimited JSON in BQ. (In practice you can still query JSONL, but be explicit.)

---

### 2) “BQ auto-infers schema from JSON” is not a free lunch

Even if autodetect is enabled, external table schema inference can be **brittle** when:

* files have inconsistent shapes across the 863 JSONs (Source B)
* arrays/objects vary record-to-record
* fields appear/disappear

This matters because your Part 3 explicitly says *don’t silently lose data* and handle dirtiness explicitly. 

**Best-practice adjustment**
For Source B, consider loading into a **native table with a JSON column** (or at least stage with JSON-typed columns) and then extract has a native `JSON` type and supports JSON extraction functions; Google explicitly documents this path. ([Google Cloud Documentation][3])

This makes schema drift easier to contain: you keep the raw JSON intact and parse only what you need.

---

### 3) Their staging layer doesn’t actually meet your canonical schema yet

Example issues:

* They keep `experienceList` / `educationList` as “JSON for now” — but your canonical schema expects **structured arrays** with deterministic `experience_id` etc. 
* They don’t include `identity_sources` and your schema explicitly wants per-source identity values for provenance. 
* They don’t produce the canonical nested objects (`location`, `social_metrics`, `c:contentReference[oaicite:11]{index=11}lds`, `provenance`, `sync_metadata`) as specified. 

**Fix**
Your “silver” tables should ped* columns (even if some are NULL), including:

* `identity_sources` array with one element per source record
* `normalization_errors` array/struct (invalid dates, pa experience/education normalized into arrays of structs (and generate deterministic IDs)

This is consistent with your profiling recommendations (parse Source 2 dates to ISO, normalize ids). 

---

### 4) The merge SQL as written won’t work correctly (and isn’t truly idempotent yet)

Big red flags:

* `source_union` computes `ROW_NUMBER()` but then doesn’t use it.
* `matched_records` is unused.
* Merge uses `LEFT JOIN stg_s:contentReference[oaicite:16]{index=16}id = s1.linkedin_num_id` while `s` is already unioned from both sources, which can create duplicates and requires a careful aggregate to collapse. Their `GROUP BY ...` is hand-wavy (“GROUP BY ..., ...”) and would either error or produce wrong rows.
* `ARRAY_CONCAT_AGG(DISTINCT s.source_system)` is not valid as written (you can’t `CONCAT_AGG` strings into an array that way; you’d use `ARRAY_AGG(DISTINCT ...)`).

**Fix (clean, deterministic merge pattern)**
Use a **FULL OUTER JOIN** between the two staging tables keyed on `linkedin_num_id`, then choose fields with explicit rules. That naturally handles “mostly distinct” (A-only and B-only rows flow through). The assignment explicitly expects that. 

Then build:

* `provenance.source_systems` as `[source_1]`, `[source_2]`, or `[source_1, source_2]`
* arrays as `ARRAY(SELECT AS STRUCT ... )` union/dedupe by deterministic IDs per your schema/rationale 

--citly” is missing in their plan
They filter out invalid records (`WHERE linkedinNumID IS NOT NULL` etc.), but **filtering isn’t “explicit handling.”**

The assignment wants you to *surface* dirtiness. 
**Best-practice approach**
Add fields like:

* `normalization_errors ARRAY<STRUCT<field STRING, error STRING, raw_value STRING>>`
* `parse_warnings ARRAY<STRING>`
* `raw_payload_json JSON` (or pointer)

So if `SAFE_CAST(linkedin_num_itill keep the record in a quarantine table, and you can report “X% invalid ids” rather than silently dropping.

This is also consistent with your “avoid silent loss” requirement from Part 2. 

---

### 6) Derived fields: UPDATE-at-end is okay, but not ideal

They propose:

* `CREATE TABLE ... AS SELECT ...`
* then `UPDATE people_canonical SET derived_fields ... WHERE TRUE`

It works, but:

* it’s slower / more expensive than c (two passes)
* it’s easier to accidentally make non-deterministic changes later

Also, “years_of_experience” is non-trivial: your example explicitly expects “sum non-overlapping periods.” 

**Fix**
Compute derived fields inside the merge SELECT (or as a separate view/materialized view) so the output is deterministic for the same inputs.

---

## Firestore-related note (because Part 3 affects Part 4)

Even though this is cipate Part 4 constraints:

Firestore has strict operational guidance: ramp write throughput and distribute writes across keyspace. ([Firebase][4])
So keep canonical nested stuff in BQ, but export a lean `firestore_export` with a `sync_hash` (which you already designed) to minimize write amplification. 

---

## Verdict on the plan (validation + edits)

**Validated:** The macro-architecture and “SQL-first” idea is absolutely reasonable and aligned with DE norms and the assignment priorities. 

**Needs correction:** The implementation details around external tables/autodetect, the mer dirtiness handling, and producing canonical-shaped nested arrays (experience/education) per your schema. 

If you want, paste just your *actual* `stg_source_1` anmas (column names + types), and I’ll give you a clean “gold merge” SQL skeleton that:

* handles A-only/B-only rows
* is deterministic/idempotent
* outputs exactly your canonical schema (including `identity_sources`, `provenance`,experience/education).

[1]: https://docs.cloud.google.com/bigquery/docs/schema-detect?utm_source=chatgpt.com "Using schema auto-detection | BigQuery"
[2]: https://docs.cloud.google.com/bigquery/docs/loading-data-cloud-storage-json?utm_source=chatgpt.com "Loading JSON data from Cloud Storage | BigQuery"
[3]: https://docs.cloud.google.com/bigquery/docs/json-data?utm_source=chatgpt.com "Working with JSON data in GoogleSQL | BigQuery"
[4]: https://firebase.google.com/docs/firestore/best-practices?utm_source=chatgpt.com "Best practices for Cloud Firestore - Firebase - Google"
