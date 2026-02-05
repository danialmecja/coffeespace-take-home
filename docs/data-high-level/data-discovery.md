# Data Discovery Report

## Overview

This document analyzes two LinkedIn profile data sources to understand their schemas, data shapes, and data quality characteristics.

| Attribute | Source 1 | Source 2 |
|-----------|----------|----------|
| **Provider Style** | Enriched/Aviato-style | Raw LinkedIn scraper |
| **Sample Subject** | Chris Craft (Sr. IT Project Manager) | Amer M. (Senior Product Recall Specialist) |
| **Naming Convention** | camelCase | snake_case |
| **Data Depth** | Deeply nested with company enrichment | Flatter structure with inline positions |
| **Lines (pretty-printed)** | 1173 | ~200 |

---

## Source 1: Enriched LinkedIn Data (Aviato-style)

### Top-Level Schema

| Field | Type | Description | Example Value |
|-------|------|-------------|---------------|
| `id` | string | Internal unique identifier | `"-eQz78jE2igUKhDbOwRcEoMG1fqOU0J"` |
| `linkedinID` | string | LinkedIn profile slug | `"chris-craft-2a0b7b4"` |
| `linkedinNumID` | number | Numeric LinkedIn ID | `20654473` |
| `linkedinEntityID` | string | LinkedIn entity URN | `"ACoAAAE5iskBN9YLGyU4preoWOcDbXvwCa0G8BA"` |
| `fullName` | string | Complete name | `"Chris Craft"` |
| `firstName` | string | First name | `"Chris"` |
| `lastName` | string | Last name | `"Craft"` |
| `headline` | string | Profile headline | `"Sr. Project Manager - SAP - Kforce"` |
| `location` | string | Location string | `"Austin, Texas, United States"` |
| `locationIDList` | number[] | Who's On First location IDs | `[102191575, 85633793, ...]` |
| `gender` | string \| null | Inferred gender | `null` |
| `linkedinConnections` | number | Connection count | `13092` |
| `linkedinFollowers` | number | Follower count | `13283` |
| `linkedinLaborStatus` | string | Employment status | `"Employed"` |
| `about` | string | Bio/summary text | Long form text |
| `skills` | string[] | Skill keywords | `["Project Management", "SAP", ...]` |
| `imageURL` | string | Profile image URL | Aviato CDN URL |
| `lastUpdated` | string (ISO8601) | Last data refresh | `"2025-10-24T05:37:55.731Z"` |

### Computed/Derived Fields

Source 1 includes enriched boolean flags computed by the data provider:

| Field | Type | Description |
|-------|------|-------------|
| `computed_likelyToExplore` | boolean | Likely open to opportunities |
| `computed_recentlyLeftCompany` | boolean | Left a company recently |
| `computed_potentialToLeave` | boolean | May leave current role |
| `computed_priorBackedFounder` | boolean | Has founded a backed company |
| `computed_unicornEarlyEngineer` | boolean | Early employee at unicorn |
| `computed_bigTechAlumPrivate` | boolean | Worked at private big tech |
| `computed_bigTechAlumPublic` | boolean | Worked at public big tech |

### Experience Structure (`experienceList`)

Each experience entry contains:

```
experienceList[]: {
    id: string              // Unique experience ID
    endDate: string|null    // ISO8601 or null if current
    startDate: string       // ISO8601
    linkedinNumID: number   // Company's LinkedIn numeric ID
    entityType: string      // Always "company"
    companyName: string     // Company display name
    companyID: string       // Internal company ID
    description: string|null
    
    positionList[]: {       // Nested positions at this company
        title: string
        startDate: string
        endDate: string|null
        location: string|null
        description: string|null
        department: string|null     // Optional
        seniorityScore: number|null // Optional (1-5 scale)
    }
    
    company: {              // FULL COMPANY OBJECT (deeply nested)
        id: string
        isStartup: boolean|null
        financingStatus: string|null  // "Corporation", "Formerly VC-backed", etc.
        linkedinID: string
        headcount: number
        name: string
        location: string
        locationIDList: number[]
        locality: string|null
        country: string
        region: string|null
        status: string              // "active"
        ownershipStatus: string|null // "Publicly Held", "Acquired/Merged", etc.
        industryList: string[]
        computed_tags: string[]     // AI-inferred tags
        founded: string|null        // ISO8601
        yearlyHeadcountChange: number
        monthlyHeadcountChange: number
        triMonthlyHeadcountChange: number
    }
}
```

### Education Structure (`educationList`, `degreeList`)

Both arrays exist but are **empty** in this sample:
- `educationList`: `[]`
- `degreeList`: `[]`

### Other Arrays

| Field | Type | Contents |
|-------|------|----------|
| `languageList` | object[] | `[{id, proficiency, languageName}]` |
| `companiesFoundedList` | array | Empty in sample |

### Location Details (`locationDetails`)

Hierarchical geographic data:

```
locationDetails: {
    continent: { id, name, placeType, geometry: { area_square_degrees, area_square_m, bbox, lat, lon } }
    country: { ... }
    region: { ... }
    county: { ... }
    locality: { ... }
}
```

---

## Source 2: LinkedIn Scraper Data

### Top-Level Schema

| Field | Type | Description | Example Value |
|-------|------|-------------|---------------|
| `id` | string | LinkedIn profile slug | `"amer-m-34b695135"` |
| `linkedin_id` | string | Same as id | `"amer-m-34b695135"` |
| `linkedin_num_id` | string | Numeric ID (as string!) | `"554095787"` |
| `input_url` | string | Source URL scraped | Full LinkedIn URL |
| `url` | string | Profile URL | Full LinkedIn URL |
| `name` | string | Full display name | `"Amer M."` |
| `first_name` | string | First name | `"Amer"` |
| `last_name` | string | Last name | `"M."` |
| `position` | string | Current position headline | `"Senior Product Recall Specialist @ Medline"` |
| `about` | string \| null | Bio text | `null` |
| `city` | string | City/metro area | `"Greater Chicago Area"` |
| `location` | string \| null | Location | `null` |
| `country_code` | string | 2-letter country code | `"US"` |
| `connections` | number | Connection count | `151` |
| `followers` | number | Follower count | `151` |
| `avatar` | string | Profile photo URL | LinkedIn CDN URL |
| `banner_image` | string | Banner photo URL | LinkedIn CDN URL |
| `default_avatar` | boolean | Using default image | `true` |
| `memorialized_account` | boolean | Is memorial account | `false` |

### Current Company (Denormalized)

Redundant current company fields:

| Field | Type |
|-------|------|
| `current_company` | object `{company_id, location, name, title}` |
| `current_company_company_id` | string (duplicate) |
| `current_company_name` | string (duplicate) |

### Experience Structure (`experience`)

Two formats exist within the same array:

**Format A: Simple (single role per company)**
```
{
    company: string           // Company name
    company_id: string|null   // LinkedIn company slug
    company_logo_url: string|null
    title: string
    start_date: string        // "Oct 2024" (human readable!)
    end_date: string          // "Present" or "Apr 2023"
    location: string|null
    description: string|null
    description_html: string|null
    url: string|null          // Company LinkedIn URL
}
```

**Format B: Grouped (multiple roles at same company)**
```
{
    company: string
    company_id: string
    company_logo_url: string|null
    title: string             // Company name repeated
    duration: string          // "1 year 1 month"
    location: string
    url: string
    
    positions[]: {            // Nested position list
        title: string
        subtitle: string      // Company name again
        start_date: string
        end_date: string
        meta: string          // "Jul 2024 - Aug 2024 2 months"
        location: string
        description: string
        description_html: string
    }
}
```

### Education Structure (`education`)

```
education[]: {
    title: string              // School name
    degree: string|null        // "Bachelor of Science - BS"
    field: string|null         // "Food Science and Human Nutrition"
    description: string|null
    description_html: string|null
    institute_logo_url: string
    url: string|null
}
```

Also has denormalized: `educations_details: string` ("University of Illinois at Urbana-Champaign")

### Certifications Structure

```
certifications[]: {
    title: string
    subtitle: string           // Issuing organization
    meta: string              // "Issued Jun 2024"
    credential_id: string|null
    credential_url: string|null
}
```

### Activity Feed

```
activity[]: {
    id: string
    title: string             // Post preview text
    img: string               // Post image URL
    link: string              // Post URL
    interaction: string       // "Liked by Amer M."
}
```

### Null/Empty Fields

Many fields are explicitly null or empty:

| Field | Value |
|-------|-------|
| `about` | `null` |
| `courses` | `null` |
| `honors_and_awards` | `null` |
| `languages` | `null` |
| `organizations` | `null` |
| `patents` | `null` |
| `people_also_viewed` | `null` |
| `posts` | `null` |
| `projects` | `null` |
| `publications` | `null` |
| `recommendations` | `null` |
| `recommendations_count` | `null` |
| `volunteer_experience` | `null` |
| `bio_links` | `[]` |
| `similar_profiles` | `[]` |

---

## Data Quality Issues

### Source 1 Issues

| Issue | Severity | Description | Examples |
|-------|----------|-------------|----------|
| **Null gender** | Low | Gender field is null | `"gender": null` |
| **Empty education** | Medium | `educationList` and `degreeList` are empty despite profile likely having education | Both arrays `[]` |
| **Inconsistent location granularity** | Low | Some companies have null `locality` or `region` | Huntsman: `"locality": null` |
| **Null `isStartup`** | Low | Startup status unknown for most companies | All show `null` |
| **Null `financingStatus`** | Low | Some companies missing financing data | Baker Hughes, Corning: `null` |
| **Null `founded`** | Low | Some companies missing founding date | Huntsman: `"founded": null` |
| **Inconsistent `ownershipStatus`** | Low | Mix of null and values | Baker Hughes: `null` vs others |
| **Future start dates** | High | Experience shows 2025 start date (data may be stale or error) | City of San Antonio: `"startDate": "2025-07-01"` |
| **Optional fields inconsistently present** | Medium | `department`, `seniorityScore` only on some positions | Only on older roles |
| **Empty `computed_tags`** | Low | Many companies have no computed tags | Multiple companies |
| **Negative headcount changes** | Info | Most companies showing declining headcount | Normal but notable |

### Source 2 Issues

| Issue | Severity | Description | Examples |
|-------|----------|-------------|----------|
| **Type inconsistency: `linkedin_num_id`** | High | Stored as string, not number | `"554095787"` vs Source 1's `20654473` |
| **Non-ISO date formats** | High | Human-readable dates, not parseable | `"Oct 2024"`, `"Present"` |
| **Redundant/denormalized data** | Medium | Same info in multiple fields | `current_company`, `current_company_name`, `current_company_company_id` |
| **Mixed experience formats** | High | Array contains two different object shapes | Format A vs Format B with `positions[]` |
| **Inconsistent `company_id`** | Medium | Some experiences missing company_id | Podiatry Homecare: no `company_id` |
| **Null `location` at profile level** | Medium | Location null despite having `city` | `"location": null`, `"city": "Greater Chicago Area"` |
| **No structured location data** | High | Only human-readable strings, no IDs or hierarchy | No equivalent to Source 1's `locationIDList` |
| **Missing `company_logo_url`** | Low | Some entries have null logo | Prinova USA: `null` |
| **`description_html` with messy HTML** | Low | Contains HTML entities and artifacts | `&#x2019;`, `<!----> ` |
| **Many null feature fields** | Info | Lots of sparse data | languages, courses, publications all null |
| **Inconsistent education schema** | Medium | Some entries missing `degree`, `field` | High school entry has no degree fields |
| **String-based durations** | Medium | Not machine-parseable | `"1 year 1 month"` |
| **Redundant `meta` field** | Low | Duplicates start/end date info | `"Jul 2024 - Aug 2024 2 months"` |
| **No enriched company data** | Info | Only company name/id/logo, no headcount, industry, etc. | By design, but limits analysis |

---

## Schema Comparison Summary

| Aspect | Source 1 | Source 2 |
|--------|----------|----------|
| **ID fields** | Multiple typed IDs (`id`, `linkedinID`, `linkedinNumID`, `linkedinEntityID`) | Redundant string IDs (`id`, `linkedin_id`, `linkedin_num_id` as string) |
| **Naming** | camelCase | snake_case |
| **Dates** | ISO8601 strings | Human-readable ("Oct 2024") |
| **Location** | Hierarchical with Who's On First IDs | Flat strings only |
| **Company data** | Full nested company objects with enrichment | Name, ID, logo URL only |
| **Experience nesting** | Consistent: experience → positionList | Inconsistent: flat OR experience → positions |
| **Computed fields** | Yes (7 boolean flags) | No |
| **Headcount/trends** | Yes (monthly/quarterly/yearly changes) | No |
| **Activity/social** | No | Yes (recent likes) |
| **Certifications** | Not visible in sample | Yes |
| **HTML content** | No | Yes (`description_html` fields) |

---

## Recommendations for ETL/Integration

1. **Normalize date formats**: Parse Source 2's human-readable dates into ISO8601
2. **Type coercion**: Convert Source 2's `linkedin_num_id` from string to number
3. **Flatten Source 1 company data**: If joining with a company dimension table, extract nested company objects
4. **Standardize experience structure**: Create unified schema handling both Source 2 formats
5. **Location resolution**: For Source 2, consider geocoding to add structured location data
6. **Handle nulls explicitly**: Both sources have sparse fields; decide on NULL vs default values
7. **Deduplicate Source 2 redundancies**: Pick canonical fields (`current_company` vs individual fields)
8. **Strip HTML artifacts**: Clean `description_html` fields or use plain `description`
9. **Validate future dates**: Flag/investigate Source 1 entries with future start dates
