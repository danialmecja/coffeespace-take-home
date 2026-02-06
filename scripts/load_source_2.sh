#!/bin/bash
# Load Source 2 into BigQuery
# Run this from Cloud Shell or a GCE VM for best performance
#
# Source 2 files are JSON arrays [...], not JSONL.
# This script converts them on-the-fly and loads into BigQuery.

set -euo pipefail

PROJECT="coffeespace-sandbox"
DATASET="coffeespace_canonical"
TABLE="raw_source_2"
SOURCE_BUCKET="gs://coffeespace-sandbox-source-2"

echo "Loading Source 2 from ${SOURCE_BUCKET}/*.json"
echo "Converting JSON arrays to NDJSON and loading into ${PROJECT}:${DATASET}.${TABLE}"

gsutil cat "${SOURCE_BUCKET}/*.json" \
  | jq -c 'if type=="array" then .[] else . end' \
  | bq load \
      --source_format=NEWLINE_DELIMITED_JSON \
      --autodetect \
      --replace \
      "${PROJECT}:${DATASET}.${TABLE}" \
      -

echo "Done. Verifying row count..."
bq query --use_legacy_sql=false \
  "SELECT COUNT(*) as row_count FROM \`${PROJECT}.${DATASET}.${TABLE}\`"
