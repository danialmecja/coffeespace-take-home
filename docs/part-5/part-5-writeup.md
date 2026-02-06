1. Every quarter, new snapshots arrive and existing records may change or disappear. How would you handle snapshot versioning, delta detection, and rollback if a bad snapshot is deployed?

Current implementation:
- pipeline uses 'provenance.last_merget_at' and source-level `last_updated` timestamps, but lacks explicit snapshot versioning. The `sync_hash` field (MD5 of all synced fields) provides O(1) change detection for incremental syncs.

Assume:
people_canonical table
  ├── Today: Load new data → replaces everything
  └── Yesterday's data: GONE FOREVER

  If today's data is bad we can't go back. There's no version history.

  Snapshot versioning approach:
  people_canonical_20260301  ← March 1st snapshot (kept)
  people_canonical_20260315  ← March 15th snapshot (kept)
  people_canonical_20260401  ← April 1st snapshot (new)

  Each quarterly load creates a new table instead of overwriting. Now we have history.

  What the Metadata Table Does

  Think of it as a logbook tracking each snapshot:
  ┌─────────────┬───────────────┬───────────┬───────────────────┬─────────────┐
  │ snapshot_id │ snapshot_date │ row_count │ validation_status │ promoted_at │
  ├─────────────┼───────────────┼───────────┼───────────────────┼─────────────┤
  │ 2026Q1      │ 2026-03-01    │ 850,000   │ promoted          │ 2026-03-05  │
  ├─────────────┼───────────────┼───────────┼───────────────────┼─────────────┤
  │ 2026Q2      │ 2026-06-01    │ 820,000   │ promoted          │ 2026-06-08  │
  ├─────────────┼───────────────┼───────────┼───────────────────┼─────────────┤
  │ 2026Q3      │ 2026-09-01    │ 580,000   │ pending           │ NULL        │
  └─────────────┴───────────────┴───────────┴───────────────────┴─────────────┘
  When Q3 arrives with only 580K records (down 30%), you can:
  1. See it's not validated yet (validation_status = 'pending')
  2. Compare to Q2's 820K (red flag!)
  3. Keep using Q2 in production while you investigate
  4. Decide whether to promote Q3 or reject it

Recommended Approach:
For snapshot versioning:
- We could partition people table (canonical) by snapshot_date instead of overwriting
- Use table naming like people_canonical_YYYMMDD or add a snapshot_id col

Delta detection:
- Compare `sync_hash` between current and previous snapshots to identify changed records
- Use BigQuery's `EXCEPT` operator for efficient row-level diffing:
- Track deltas in a `people_changelog` table for audit trails

Rollback Strategy:
- Keep last N snapshots (e.g., 4 quarters = 1 year retention)
- Rollback = atomically swap view pointer:


2. Before promoting new data to the serving layer, what validation and data quality checks would you run to prevent bad data from reaching users?

Current implementation:
- We track `normalization_errors` during ETL (NULL values, invalid timestamps, whitespace issues) but lack comprehensive pre-promotion validation.

Recommended Approach:
- Schema validation
- Statistical Validation
  - Row count within exp. range
  - Have null rate thresholds per critical field (like full_name null_rate< 1%)
  - Cardinality checks (unique 'linkedin_id' count should match total rows)
  - Value dist. checks (e.g., primary_portfolio should have known categories only)
- Business Logic Validation - sanity checks on derived fields

A way of automating this is maybe through data quality gates. Online research posits:
Using [CloudDQ](https://github.com/GoogleCloudPlatform/cloud-data-quality) to define rules as YAML and fail pipeline if thresholds breached:

```yaml
rules:
  - rule_id: null_pk_check
    dimension: completeness
    table: people_canonical_SNAPSHOT
    column: linkedin_id
    condition: null_rate < 0.001
    severity: CRITICAL

  - rule_id: record_count_stability
    dimension: consistency
    table: people_canonical_SNAPSHOT
    condition: row_count BETWEEN prev_snapshot_count * 0.9 AND prev_snapshot_count * 1.1
    severity: HIGH
```


3. A vendor delivers a new periodic snapshot with 30% fewer records than the previous one. How do you determine whether this is a bad export or a legitimate change, and how should the pipeline handle each case?

Have a decision-making framework
Drops could indicate:
- Bad export: Vendor pipeline failure, incomplete file transfers
- A legit change (vendor-related): priv. compliance, data retention policies from vendors

To detect and triage:
Snapshot-referenced detection
- We use current snapshot and get current count -> compare agains prev. snapshot's count and find pct_change and if its above a certain threshold, flag it

Then, check deletion patterns (what records disappeared?)
- Choose unique linkedin_ids from say 2 snapshots, find missing counts on records like derived fields for instance
- If deletions are clustered (say from one country only), likely a bad export. If distributed, then may be legit.

Another is to check raw source level counts:
- If 1 source dropped 60% but the other is alright, isolate the bad source

Check and validate file metadata
- Check GCS file sizes vs. prev. snapshot
- Verify file timestamps

Handling the pipeline:
- If bad export
  - Block validation promotion (validation_status=failed)
  - Alert data team + vendor
  - Keep prev. snapshot in prod
  - Request re-export from vendor
  - Document incident in snapshot_metadata (notes)
- If legit change:
  - Require manual approval before promotion
  - Document business justification
  - Notify downstream consumers
  - Promore to prod but flag as 'high_delta'
- If uncertain
  - Run additional validation
    - Sample "missing" IDs and manually verify on LinkedIn
    - Check correlation with known events

4. On deduplication: suppose Vendor B has more recent data for a candidate initially sourced from Vendor A, but some fields are null or missing compared to Vendor A’s version. How would you design your merge logic to handle this?

Current Implementation Context

Our merge logic uses simple COALESCE preference: `COALESCE(s1.identity.full_name, s2.identity.full_name)` always picks Source 1 if non-null. This works when Source 1 is consistently more complete, but breaks if Source 2 has newer data with nulls.

The Problem Illustrated

```
Source 1 (Aviato, older):
  linkedin_id: "john-doe-123"
  full_name: "John Doe"
  headline: "Software Engineer at Google"
  about: "10 years experience..."
  last_updated: 2026-01-15

Source 2 (Scraper, newer):
  linkedin_id: "john-doe-123"
  full_name: "John Doe"
  headline: "Senior Engineer at Meta"  ← Updated!
  about: NULL                           ← Missing!
  last_updated: 2026-03-20
```

**Current logic:** Would keep "Software Engineer at Google" (wrong - outdated) because Source 1 is preferred.

**Desired outcome:** Keep "Senior Engineer at Meta" (correct - newer), but preserve "10 years experience..." (don't replace with NULL).

Recommended Approach:
Instead of source-level preference, implement field-level recency with null-safety

5. You need to add a third data source with a completely different schema next quarter. What in your current pipeline design accommodates this, and what would need to change?

Our pipeline has hardcoded assumptions about two sources:
- `stg_source_1` and `stg_source_2` tables
- FULL OUTER JOIN on `linkedin_id`
- Source-specific field mappings in staging SQL

What would accommodate a third source?
- Abstracting the staging/silver layer
  - Each source has its own staging SQL that normalises to the main/ideal canonical schema
    - canon_schema is source agnostic (no source-1 specific fields)
  - Merge logic operated on canon structs, not raw source fields
- Join key standardisation
  - linkedin_id is the universal join key
  - if source 3 has that, it can participate

What would need to change:
Staging Layer code
- map source 3's join key to canon linkedin_id -> map to canonical identity struct -> map to canonical location struct + any other remaining fields mapping to canon schema

Merge Logic (we'd extend the FULL OUTER JOIN to three-ways)
-- Current: two-way join
FROM stg_source_1 s1
FULL OUTER JOIN stg_source_2 s2
  ON s1.linkedin_id = s2.linkedin_id

-- New: three-way join (requires two joins)
FROM stg_source_1 s1
FULL OUTER JOIN stg_source_2 s2
  ON s1.linkedin_id = s2.linkedin_id
FULL OUTER JOIN stg_source_3 s3
  ON COALESCE(s1.linkedin_id, s2.linkedin_id) = s3.linkedin_id

Then, update field resolution to consider three sources
- Change the STRUCT
STRUCT(
  COALESCE(s1.identity.full_name, s2.identity.full_name, s3.identity.full_name) AS full_name,
  -- Apply temporal preference across all three if using recency-based logic
) AS identity

Then update provenance tracking
- Update `source_systems` array logic:

CASE
  WHEN s1.linkedin_id IS NOT NULL
   AND s2.linkedin_id IS NOT NULL
   AND s3.linkedin_id IS NOT NULL
    THEN ['source_1', 'source_2', 'source_3']
  WHEN s1.linkedin_id IS NOT NULL
   AND s2.linkedin_id IS NOT NULL
    THEN ['source_1', 'source_2']
  -- ... handle all 7 combinations ...
END AS source_systems

6. Your sync handles the current dataset. How would you modify it to scale to tens of millions of records? What would change in your batching strategy, error handling, and infrastructure?

Current implementation:
- likely syncs full dataset each run
- At tens of millions, might take a bit too long
  - plus BQ has query result size limits
  - firestore has write quotas
  - memory exhaustion

Batching strat. changes:
- Consider delta-only sync
  - Only sync changed records using sync_hash
    - If only 100K records changed (which is like 1% delta), thats good for sync time
- Process in chunks of 10-50K records in parallel
- Use dist. computing (CloudRun) and do like 100 CR jobs each sycning 1/100th of the data in parallel

For error handling:
- Right now if any record fails, entire sync fails

Can consider:
- Retries with exponential backoffs
  - exp. spread out retries to the service
- Have idempotency tokens in case network lags and there are double-counts
  - If FS hiccups, there 