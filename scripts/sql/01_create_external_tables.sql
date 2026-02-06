-- Step 1: Create External Tables (Bronze Layer)
-- Zero-copy access to GCS data with schema autodetection
-- Source 1: Aviato-style enriched data (JSONL format)
-- Note: Uses linkedinID (URL slug) as the join key, not linkedinNumID
-- Source 2 is loaded via Python (JSON arrays, not JSONL)

CREATE OR REPLACE EXTERNAL TABLE `coffeespace-sandbox.coffeespace_canonical.raw_source_1`
OPTIONS (
  format = 'JSON',
  uris = ['gs://coffeespace-sandbox-source-1/CoffeeSpaceTestDatav4.jsonl'],
  max_bad_records = 1000,
  ignore_unknown_values = true
)
