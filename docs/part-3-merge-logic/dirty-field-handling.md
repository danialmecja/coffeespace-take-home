# Dirty Field Handling

This document describes how the pipeline explicitly handles dirty, inconsistent, or problematic data.

## Philosophy

Per the assignment: **"Handle dirty or inconsistent fields explicitly"**

Rather than silently dropping or mutating data, we:
1. Track all normalization issues in `normalization_errors` array
2. Apply minimal cleaning (whitespace normalization)
3. Use `SAFE_CAST` / `SAFE.PARSE_*` to avoid failures
4. Filter only truly invalid records (NULL primary key)

## Normalization Errors Tracked

Each record carries an array of encountered issues:

```sql
normalization_errors ARRAY<STRUCT<
  field STRING,      -- Which field had the issue
  error STRING,      -- Error type
  raw_value STRING   -- Original value for debugging
>>
```

### Source 1 Errors

| Error Type | Field | Description |
|------------|-------|-------------|
| `NULL_VALUE` | `linkedin_num_id` | Primary key is NULL (record filtered) |
| `INVALID_TIMESTAMP` | `last_updated` | Date string couldn't be parsed |

### Source 2 Errors

| Error Type | Field | Description |
|------------|-------|-------------|
| `INVALID_INT64` | `linkedin_num_id` | String couldn't be cast to INT64 (record filtered) |
| `EXTRA_WHITESPACE` | `full_name` | Name contains multiple consecutive spaces |

## Cleaning Applied

We apply minimal, explicit cleaning:

### Whitespace Normalization
```sql
-- Source 2 names often have extra spaces
TRIM(REGEXP_REPLACE(name, r'\s+', ' ')) AS full_name
```

This is tracked in `normalization_errors` when applied.

### Date Parsing

Source 2 uses human-readable dates ("Oct 2024", "Present"):

```sql
-- Safe parsing with explicit format
SAFE.PARSE_DATE('%b %Y', starts_at) AS start_date

-- Handle "Present" explicitly
IF(ends_at = 'Present', NULL, SAFE.PARSE_DATE('%b %Y', ends_at)) AS end_date
```

Failed parses result in NULL, not errors.

### Type Coercion

Source 2 stores `linkedin_num_id` as STRING:

```sql
SAFE_CAST(linkedin_num_id AS INT64) AS linkedin_num_id
```

Invalid casts are tracked and the record is filtered.

## What We DON'T Do

1. **Silent mutation**: We don't change values without tracking
2. **Over-cleaning**: We don't normalize case, remove special characters, etc.
3. **Guessing**: We don't infer missing values
4. **Silent drops**: Filtered records are documented

## Querying Dirty Data

To find records with issues:

```sql
-- All records with any normalization errors
SELECT linkedin_num_id, normalization_errors
FROM people_canonical
WHERE ARRAY_LENGTH(normalization_errors) > 0;

-- Records with specific error types
SELECT *
FROM people_canonical, UNNEST(normalization_errors) AS err
WHERE err.error = 'EXTRA_WHITESPACE';

-- Error distribution
SELECT
  err.field,
  err.error,
  COUNT(*) as occurrences
FROM people_canonical, UNNEST(normalization_errors) AS err
GROUP BY 1, 2
ORDER BY 3 DESC;
```

## Validation Queries

Pre-staging validation (02_validate_sources.sql) checks:

```sql
-- Source 1: Critical field completeness
COUNTIF(linkedinNumID IS NULL) AS missing_linkedin_num_id

-- Source 2: Type consistency
COUNTIF(SAFE_CAST(linkedin_num_id AS INT64) IS NULL
        AND linkedin_num_id IS NOT NULL) AS bad_linkedin_num_id_type
```

This surfaces issues before staging, not after.
