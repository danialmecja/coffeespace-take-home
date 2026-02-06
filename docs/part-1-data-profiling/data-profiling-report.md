# Data Profiling Report

**Generated**: 2026-02-05T20:32:49.777924
**Methodology**: Statistical sampling from GCS sources

## Executive Summary

| Metric | Source 1 (Aviato) | Source 2 (LinkedIn Scraper) |
|--------|-------------------|------------------------------|
| **Records Sampled** | 10,000 | 10,000 |
| **Top-Level Fields** | 30 | 41 |
| **Quality Issues Found** | 19 | 86 |

---

## Source 1: Aviato-Style Enriched Data

### Schema Characteristics
- **Naming convention**: camelCase
- **Date format**: ISO8601 (e.g., `2025-10-24T05:37:55.731Z`)
- **Structure**: Deeply nested with embedded company objects
- **Identifiers**: Multiple ID fields (`id`, `linkedinID`, `linkedinNumID`, `linkedinEntityID`)

### Top-Level Field Completeness

| Field | Present | Null Rate | Empty Rate | Completeness |
|-------|---------|-----------|------------|--------------|
| `about` | 100% | 15.5% | 0.0% | 84.5% |
| `companiesFoundedList` | 100% | 0.0% | 97.6% | 2.4% |
| `computed_bigTechAlumPrivate` | 100% | 15.3% | 0.0% | 84.7% |
| `computed_bigTechAlumPublic` | 100% | 15.3% | 0.0% | 84.7% |
| `computed_likelyToExplore` | 100% | 15.3% | 0.0% | 84.7% |
| `computed_potentialToLeave` | 100% | 15.3% | 0.0% | 84.7% |
| `computed_priorBackedFounder` | 100% | 15.3% | 0.0% | 84.7% |
| `computed_recentlyLeftCompany` | 100% | 15.3% | 0.0% | 84.7% |
| `computed_unicornEarlyEngineer` | 100% | 15.3% | 0.0% | 84.7% |
| `degreeList` | 100% | 0.0% | 13.4% | 86.6% |
| `educationList` | 100% | 0.0% | 5.5% | 94.5% |
| `experienceList` | 100% | 0.0% | 1.9% | 98.1% |
| `firstName` | 100% | 0.4% | 0.0% | 99.6% |
| `fullName` | 100% | 0.0% | 0.0% | 100.0% |
| `gender` | 100% | 99.3% | 0.0% | 0.7% |
| `headline` | 100% | 0.2% | 0.0% | 99.8% |
| `id` | 100% | 0.0% | 0.0% | 100.0% |
| `imageURL` | 100% | 0.2% | 0.0% | 99.8% |
| `languageList` | 100% | 0.0% | 74.6% | 25.4% |
| `lastName` | 100% | 0.4% | 0.0% | 99.6% |
| `lastUpdated` | 100% | 0.0% | 0.0% | 100.0% |
| `linkedinConnections` | 100% | 0.0% | 0.0% | 100.0% |
| `linkedinEntityID` | 100% | 0.4% | 0.0% | 99.6% |
| `linkedinFollowers` | 100% | 0.0% | 0.0% | 100.0% |
| `linkedinID` | 100% | 0.0% | 0.0% | 100.0% |
| `linkedinLaborStatus` | 100% | 18.3% | 0.0% | 81.7% |
| `location` | 100% | 0.0% | 0.0% | 100.0% |
| `locationDetails` | 100% | 0.0% | 0.0% | 100.0% |
| `locationIDList` | 100% | 0.0% | 0.0% | 100.0% |
| `skills` | 100% | 0.0% | 1.2% | 98.8% |

### Data Quality Issues (Top 5)

| Rank | Field | Issue | Severity | Evidence |
|------|-------|-------|----------|----------|
| 1 | `experienceList[].description` | HIGH_NULL_RATE | HIGH | 100.0% null (n=10,000) |
| 2 | `gender` | HIGH_NULL_RATE | HIGH | 99.3% null (n=10,000) |
| 3 | `companiesFoundedList` | HIGH_EMPTY_RATE | MEDIUM | 97.6% empty (n=10,000) |
| 4 | `educationList[].activities` | HIGH_NULL_RATE | MEDIUM | 79.1% null (n=10,000) |
| 5 | `educationList[].description` | HIGH_NULL_RATE | MEDIUM | 82.2% null (n=10,000) |

---

## Source 2: LinkedIn Scraper Data

### Schema Characteristics
- **Naming convention**: snake_case
- **Date format**: Human-readable (e.g., `"Oct 2024"`, `"Present"`)
- **Structure**: Flatter with denormalized fields
- **Identifiers**: `id`, `linkedin_id` (same), `linkedin_num_id` (STRING type)

### Top-Level Field Completeness

| Field | Present | Null Rate | Empty Rate | Completeness |
|-------|---------|-----------|------------|--------------|
| `about` | 100% | 59.5% | 0.0% | 40.5% |
| `activity` | 100% | 52.6% | 0.0% | 47.4% |
| `avatar` | 100% | 0.0% | 0.0% | 100.0% |
| `banner_image` | 100% | 0.0% | 0.0% | 100.0% |
| `bio_links` | 100% | 0.5% | 87.2% | 12.3% |
| `certifications` | 100% | 84.0% | 0.0% | 16.0% |
| `city` | 100% | 0.0% | 0.0% | 100.0% |
| `connections` | 100% | 14.7% | 0.0% | 85.3% |
| `country_code` | 100% | 0.0% | 0.0% | 100.0% |
| `courses` | 100% | 94.7% | 0.0% | 5.3% |
| `current_company` | 100% | 0.0% | 0.0% | 100.0% |
| `current_company_company_id` | 100% | 41.6% | 0.0% | 58.4% |
| `current_company_name` | 100% | 6.6% | 0.0% | 93.4% |
| `default_avatar` | 100% | 0.1% | 0.0% | 99.9% |
| `education` | 100% | 37.2% | 0.0% | 62.8% |
| `educations_details` | 100% | 37.4% | 0.0% | 62.6% |
| `experience` | 100% | 6.4% | 0.0% | 93.6% |
| `first_name` | 100% | 0.8% | 0.0% | 99.2% |
| `followers` | 100% | 13.7% | 0.0% | 86.3% |
| `honors_and_awards` | 100% | 92.9% | 0.0% | 7.1% |
| `id` | 100% | 0.0% | 0.0% | 100.0% |
| `input_url` | 100% | 0.0% | 0.0% | 100.0% |
| `languages` | 100% | 86.3% | 0.0% | 13.7% |
| `last_name` | 100% | 0.8% | 0.0% | 99.2% |
| `linkedin_id` | 100% | 0.0% | 0.0% | 100.0% |
| `linkedin_num_id` | 100% | 0.0% | 0.0% | 100.0% |
| `location` | 100% | 17.3% | 0.0% | 82.7% |
| `memorialized_account` | 100% | 0.2% | 0.0% | 99.8% |
| `name` | 100% | 0.0% | 0.0% | 100.0% |
| `organizations` | 100% | 94.9% | 0.0% | 5.1% |
| `patents` | 100% | 99.4% | 0.0% | 0.6% |
| `people_also_viewed` | 100% | 39.6% | 0.0% | 60.4% |
| `position` | 100% | 0.0% | 0.0% | 100.0% |
| `posts` | 100% | 96.5% | 0.0% | 3.5% |
| `projects` | 100% | 92.7% | 0.0% | 7.3% |
| `publications` | 100% | 97.0% | 0.0% | 3.0% |
| `recommendations` | 100% | 88.6% | 0.0% | 11.3% |
| `recommendations_count` | 100% | 88.6% | 0.0% | 11.4% |
| `similar_profiles` | 100% | 0.1% | 99.1% | 0.9% |
| `url` | 100% | 0.0% | 0.0% | 100.0% |
| `volunteer_experience` | 100% | 89.8% | 0.0% | 10.2% |

### Data Quality Issues (Top 5)

| Rank | Field | Issue | Severity | Evidence |
|------|-------|-------|----------|----------|
| 1 | `courses` | HIGH_NULL_RATE | HIGH | 94.7% null (n=10,000) |
| 2 | `honors_and_awards` | HIGH_NULL_RATE | HIGH | 92.9% null (n=10,000) |
| 3 | `organizations` | HIGH_NULL_RATE | HIGH | 94.9% null (n=10,000) |
| 4 | `organizations[].membership_number` | HIGH_NULL_RATE | HIGH | 100.0% null (n=10,000) |
| 5 | `patents` | HIGH_NULL_RATE | HIGH | 99.4% null (n=10,000) |

---

## Cross-Source Comparison

### Field Reliability Assessment

| Data Category | More Reliable Source | Rationale |
|---------------|---------------------|-----------|
| **Dates** | Source 1 | ISO8601 format is machine-parseable; Source 2 uses "Oct 2024" strings |
| **Location** | Source 1 | Hierarchical with Who's On First IDs; Source 2 has only flat strings |
| **Company Data** | Source 1 | Full nested objects with headcount, industry, financing status |
| **Profile IDs** | Source 1 | Consistent numeric types; Source 2 has `linkedin_num_id` as STRING |
| **Experience** | Source 1 | Consistent nested structure; Source 2 has two different formats |

### Where Sources Add Unique Coverage

| Field Category | Source 1 Only | Source 2 Only |
|----------------|---------------|---------------|
| Company enrichment (headcount, industry, tags) | ✅ | ❌ |
| Computed talent signals (`computed_*`) | ✅ | ❌ |
| Structured location hierarchy | ✅ | ❌ |
| Certifications | ❌ | ✅ |
| Activity feed (likes, posts) | ❌ | ✅ |
| Profile images (avatar, banner) | ❌ | ✅ |

### Potential Conflict Areas

| Field | Source 1 | Source 2 | Conflict Type |
|-------|----------|----------|---------------|
| Name | `fullName`, `firstName`, `lastName` | `name`, `first_name`, `last_name` | Naming + potential value differences |
| Connections | `linkedinConnections` (number) | `connections` (number) | Values may differ by scrape date |
| Location | `location` + `locationDetails` | `city`, `location`, `country_code` | Granularity mismatch |

---

## Methodology Notes

1. **Sample Size**: 10,000 records from Source 1, 10,000 from Source 2
2. **Sampling Method**:
   - Source 1: First N records streamed from 11GB JSONL
   - Source 2: Random sample of 50 files from 863 total, ~200 records each
3. **Limitations**:
   - First-N sampling may introduce ordering bias (Source 1)
   - Cannot detect cross-source duplicates without full dataset join
   - Schema observations valid; quality metrics are estimates

---

## Recommendations for Canonical Schema

1. **Primary key**: Use `linkedin_num_id` (normalize Source 2 from string to int)
2. **Date handling**: Parse Source 2 dates to ISO8601 during ETL
3. **Location**: Use Source 1's hierarchical structure as canonical; geocode Source 2
4. **Company data**: Enrich Source 2 records with Source 1's company metadata if matched
5. **Provenance**: Track `source_system` and `last_updated` for merge conflict resolution
