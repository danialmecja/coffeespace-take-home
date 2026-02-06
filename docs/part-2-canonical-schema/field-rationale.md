# Field-Level Rationale

This document explains the design decisions for each field in the canonical schema.

## Architecture Decision: 2-Layer Model

| Layer | Purpose | Optimization Target |
|-------|---------|---------------------|
| **people_canonical** | Source of truth, full provenance | Correctness, auditability |
| **firestore_export** (view) | Sync to Firestore | Delta detection, batching |

**Why 2 layers?** The CRITICAL priority is fixing BQ → Firestore sync (slow/expensive/brittle). Sync performance depends on delta detection + batching, not BQ schema structure. Keeping them separate lets us:
- Use nested STRUCTs for modeling correctness in canonical layer
- Use flat projections with `sync_hash` for efficient change detection in export layer

---

## Primary Identifiers

| Field | Type | Rationale |
|-------|------|-----------|
| `canonical_id` | STRING | Deterministic hash of `linkedin_num_id`. Provides stable PK even if LinkedIn changes their ID format. |
| `linkedin_num_id` | INT64 | Natural key present in both sources. Source 2 stores as STRING but represents same value. Use for cross-source joins. |

**Why not use LinkedIn string ID?** The `linkedinID` (Source 1) and `linkedin_id` (Source 2) are URL slugs that can change (e.g., user changes vanity URL). The numeric ID is immutable.

---

## Identity Fields

| Field | Resolution Rule | Evidence |
|-------|-----------------|----------|
| `identity.full_name` | Prefer Source 1 | 99.6% complete (vs 99.2% Source 2), camelCase naming suggests more structured processing |
| `identity.first_name` | Prefer Source 1 | 99.6% complete |
| `identity.last_name` | Prefer Source 1 | 99.6% complete |
| `identity.headline` | Prefer Source 1 | 99.8% complete |
| `identity.about` | Prefer Source 1 | 84.5% complete vs 40.5% in Source 2 |

**Why keep `identity_sources` array?** Sources may have different values (name spelling, headline updates). Storing both:
1. Enables audit of merge decisions
2. Allows re-running resolution with different rules
3. Never silently loses data

---

## Location

| Field | Source | Rationale |
|-------|--------|-----------|
| `location.display_string` | Both | Human-readable location string |
| `location.country` | Source 1 preferred | Part of hierarchical `locationDetails` object |
| `location.region` | Source 1 only | State/province from `locationDetails` |
| `location.locality` | Source 1 only | City from `locationDetails` |
| `location.country_code` | Both | ISO country code |
| `location.location_ids` | Source 1 only | Who's On First IDs for geocoding/joins |

**Why prefer Source 1 for location?** Source 1 provides structured hierarchy (`locationDetails` with country/region/locality) plus Who's On First IDs. Source 2 only has flat strings (`city`, `location`).

---

## Social Metrics

| Field | Resolution Rule | Rationale |
|-------|-----------------|-----------|
| `social_metrics.connections` | Most recent `last_updated` | Time-sensitive metric; newer = more accurate |
| `social_metrics.followers` | Most recent `last_updated` | Time-sensitive metric |
| `social_metrics.metrics_as_of` | Timestamp of source used | Enables freshness tracking |

---

## Experience

| Field | Rationale |
|-------|-----------|
| `experience_id` | Deterministic hash of `company_linkedin_id + title + start_date` for deduplication |
| `company_name` | Company display name |
| `company_linkedin_id` | For enrichment joins to company data |
| `title` | Job title |
| `start_date` | DATE (parsed from Source 2's "Oct 2024" strings) |
| `end_date` | DATE, null = current position |
| `location` | Work location (may differ from profile location) |
| `description` | Job description (100% null in Source 1, sparse in Source 2) |
| `is_current` | Derived: `end_date IS NULL` |
| `source_system` | Track which source contributed this record |

**Merge strategy**: Union from both sources, dedupe by `experience_id`. If same position appears in both, prefer Source 1 (better date parsing, company enrichment).

---

## Education

| Field | Rationale |
|-------|-----------|
| `education_id` | Deterministic hash for deduplication |
| `institution_name` | School name |
| `degree` | Degree type (BS, MS, PhD, etc.) |
| `field_of_study` | Major/concentration |
| `start_date` | DATE |
| `end_date` | DATE |
| `source_system` | Track provenance |

**Merge strategy**: Same as experience - union and dedupe.

---

## Certifications

| Field | Source | Rationale |
|-------|--------|-----------|
| All certification fields | Source 2 only | Source 1 doesn't capture certifications. 16% of Source 2 profiles have them. |

---

## Skills

Merged array from both sources, deduplicated. Both sources have ~99% completeness for skills.

---

## Computed Signals

| Field | Source | Rationale |
|-------|--------|-----------|
| `computed_signals.*` | Source 1 only | These are Aviato-computed talent signals. 84.7% completeness. High-value enrichment not available elsewhere. |

Fields preserved:
- `likely_to_explore` - Job-seeking propensity
- `recently_left_company` - Employment status change
- `potential_to_leave` - Attrition risk
- `prior_backed_founder` - VC-backed founder history
- `unicorn_early_engineer` - Early employee at unicorn
- `big_tech_alum_private` - Big tech alum now at private company
- `big_tech_alum_public` - Big tech alum now at public company

---

## Derived Fields (Assignment Requirement)

| Field | Computation | Rationale |
|-------|-------------|-----------|
| `primary_portfolio` | Categorize by most recent/longest job titles | Required by assignment. Categories: Software Engineering, Data Science, Product Management, Design, Operations, Sales, Marketing, Finance, Human Resources, Executive, Other |
| `years_of_experience` | Sum of non-overlapping employment periods | Required by assignment. Handles career gaps and overlapping positions. |
| `computation_method` | String describing algorithm version | Audit trail for derived values |

---

## Provenance

| Field | Rationale |
|-------|-----------|
| `source_systems` | Array of contributing sources (["source_1"], ["source_2"], or ["source_1", "source_2"]) |
| `source_1_id` | Original ID from Source 1 (`id` field) |
| `source_2_id` | Original ID from Source 2 (`id` field) |
| `source_1_last_updated` | When Source 1 record was last updated |
| `source_2_last_updated` | When Source 2 record was last updated |
| `first_seen_at` | When this person first appeared in any source |
| `last_merged_at` | When canonical record was last recomputed |
| `record_version` | Monotonic version number for optimistic locking |

---

## Sync Metadata

| Field | Rationale |
|-------|-----------|
| `firestore_doc_id` | Firestore document ID (deterministic from `linkedin_num_id`) |
| `last_synced_at` | When record was last synced to Firestore |
| `sync_hash` | MD5 hash of all synced fields - enables O(1) change detection |

**Why `sync_hash`?** Comparing hashes is cheaper than comparing all fields. Part 4 sync can:
1. Query `WHERE sync_hash != last_synced_hash`
2. Only write changed records to Firestore
3. Avoid write amplification from unchanged records

---

## Fields NOT Included (and why)

| Omitted Field | Source | Rationale |
|---------------|--------|-----------|
| `activity`, `posts` | Source 2 | High null rate (96.5%), not needed for serving |
| `avatar`, `banner_image` | Source 2 | Image URLs; store separately if needed |
| `bio_links` | Source 2 | 87% empty, low value |
| `courses`, `patents`, `publications` | Source 2 | 94-99% null, not worth schema complexity |
| `honors_and_awards` | Source 2 | 93% null |
| `organizations` | Source 2 | 95% null |
| `people_also_viewed` | Source 2 | Recommendation data, not profile data |
| `volunteer_experience` | Source 2 | 90% null |
| `gender` | Source 1 | 99.3% null |
| `companiesFoundedList` | Source 1 | 97.6% empty |
| `languageList` | Source 1 | 75% empty, not needed for serving |
