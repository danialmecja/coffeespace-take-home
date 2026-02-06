#!/usr/bin/env python3
"""
Parallel streaming insert for Source 2.
Bypasses GCS write requirement using BigQuery streaming API.

Run from Cloud Shell for best performance:
    python3 load_source_2_streaming.py
"""

import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.cloud import bigquery, storage
import io

PROJECT = "coffeespace-sandbox"
DATASET = "coffeespace_canonical"
TABLE = "raw_source_2"
BUCKET = "coffeespace-sandbox-source-2"
MAX_WORKERS = 20  # Parallel file processing


def load_one_file(blob, bq_client, table_ref):
    """Load a single JSON file into BigQuery via streaming insert."""
    try:
        content = blob.download_as_text()
        records = json.loads(content)
        if not isinstance(records, list):
            records = [records]

        if records:
            errors = bq_client.insert_rows_json(table_ref, records)
            if errors:
                return (blob.name, f"Insert errors: {errors[:2]}")
        return (blob.name, len(records))
    except Exception as e:
        return (blob.name, f"Error: {e}")


def main():
    print("=" * 60)
    print("Source 2 Parallel Streaming Loader")
    print("=" * 60)

    bq = bigquery.Client(project=PROJECT)
    gcs = storage.Client(project=PROJECT)
    bucket = gcs.bucket(BUCKET)

    table_ref = f"{PROJECT}.{DATASET}.{TABLE}"

    # List all JSON files
    print(f"\nListing files in gs://{BUCKET}/...")
    blobs = list(bucket.list_blobs(match_glob="*.json"))
    print(f"Found {len(blobs)} JSON files")

    if not blobs:
        print("ERROR: No JSON files found!")
        return 1

    # Create table from first file (streaming requires existing table)
    print(f"\nStep 1: Creating table from first file...")
    first_blob = blobs[0]
    first_content = json.loads(first_blob.download_as_text())
    first_records = first_content if isinstance(first_content, list) else [first_content]

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    ndjson = "\n".join(json.dumps(r) for r in first_records)
    job = bq.load_table_from_file(
        io.BytesIO(ndjson.encode()),
        table_ref,
        job_config=job_config,
    )
    job.result()
    print(f"  Created table from {first_blob.name} ({len(first_records)} rows)")

    # Stream remaining files in parallel
    remaining = blobs[1:]
    if not remaining:
        print("\nOnly one file - done!")
        return 0

    print(f"\nStep 2: Streaming {len(remaining)} files with {MAX_WORKERS} workers...")
    table = bq.get_table(table_ref)

    loaded = len(first_records)
    failed = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(load_one_file, blob, bq, table): blob.name
            for blob in remaining
        }

        for i, future in enumerate(as_completed(futures), 1):
            name, result = future.result()
            if isinstance(result, int):
                loaded += result
                if i % 50 == 0 or i == len(remaining):
                    print(f"  Progress: {i}/{len(remaining)} files, {loaded:,} total rows")
            else:
                failed.append((name, result))
                if len(failed) <= 3:
                    print(f"  WARN: {name}: {result}")

    # Summary
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"Total rows loaded: {loaded:,}")
    print(f"Files processed: {len(blobs)}")
    print(f"Failed files: {len(failed)}")

    if failed:
        print("\nFailed files (first 10):")
        for name, err in failed[:10]:
            print(f"  {name}: {err}")

    # Verify
    print("\nVerifying row count...")
    query = f"SELECT COUNT(*) as cnt FROM `{table_ref}`"
    result = list(bq.query(query).result())
    print(f"Table row count: {result[0].cnt:,}")

    return 0 if not failed else 1


if __name__ == "__main__":
    exit(main())
