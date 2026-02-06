#!/usr/bin/env python3
"""
Part 3: Cleaning, Normalization & Merge Logic Pipeline

Orchestrates the BigQuery SQL pipeline:
1. Create external table for Source 1 (bronze)
2. [PREREQ] Source 2 must be loaded via scripts/load_source_2.sh from Cloud Shell
3. Validate source data
4. Stage Source 1 (silver)
5. Stage Source 2 (silver)
6. Merge to canonical (gold)
7. Compute derived fields
8. Create Firestore export view

Prerequisites:
    # Run from Cloud Shell (keeps traffic inside GCP):
    chmod +x scripts/load_source_2.sh
    ./scripts/load_source_2.sh

Usage:
    uv run python scripts/part3_pipeline.py
"""

import sys
from pathlib import Path
from google.cloud import bigquery

# Configuration
PROJECT_ID = "coffeespace-sandbox"
DATASET_ID = "coffeespace_canonical"
SQL_DIR = Path(__file__).parent / "sql"


def run_sql_file(client: bigquery.Client, sql_file: Path, description: str) -> dict:
    """Execute a SQL file and return results summary."""
    print(f"\n{'='*60}")
    print(f"Step: {description}")
    print(f"File: {sql_file.name}")
    print("="*60)

    sql = sql_file.read_text()

    # Split on semicolons to handle multiple statements
    statements = [s.strip() for s in sql.split(";") if s.strip()]

    results = {"success": True, "statements": len(statements), "errors": []}

    for i, statement in enumerate(statements, 1):
        if not statement:
            continue

        try:
            print(f"  Executing statement {i}/{len(statements)}...")
            query_job = client.query(statement)
            query_job.result()  # Wait for completion

            if query_job.errors:
                results["errors"].extend(query_job.errors)
                print(f"    Warning: {query_job.errors}")
            else:
                # Try to get row count for SELECT/CREATE statements
                if hasattr(query_job, 'total_rows') and query_job.total_rows is not None:
                    print(f"    Rows affected: {query_job.total_rows}")
                else:
                    print(f"    Completed successfully")

        except Exception as e:
            results["success"] = False
            results["errors"].append(str(e))
            print(f"    ERROR: {e}")

    return results


def check_source_2_loaded(bq_client: bigquery.Client) -> dict:
    """
    Verify Source 2 was loaded via load_source_2.sh.

    Source 2 files are JSON arrays [...], not JSONL, so they must be
    loaded via the bash script (which runs from Cloud Shell).
    """
    print(f"\n{'='*60}")
    print("Step: Verify Source 2 Loaded")
    print("="*60)

    results = {"success": True, "errors": [], "row_count": 0}

    try:
        table_id = f"{PROJECT_ID}.{DATASET_ID}.raw_source_2"
        query = f"SELECT COUNT(*) as cnt FROM `{table_id}`"
        result = list(bq_client.query(query).result())
        row_count = result[0].cnt if result else 0

        if row_count == 0:
            print("  ERROR: raw_source_2 table is empty or doesn't exist!")
            print("  Run scripts/load_source_2.sh from Cloud Shell first.")
            results["success"] = False
        else:
            results["row_count"] = row_count
            print(f"  OK: raw_source_2 has {row_count:,} rows")

    except Exception as e:
        results["success"] = False
        results["errors"].append(str(e))
        print(f"  ERROR: {e}")
        print("  Run scripts/load_source_2.sh from Cloud Shell first.")

    return results


def verify_table_counts(client: bigquery.Client):
    """Verify row counts across pipeline stages."""
    print(f"\n{'='*60}")
    print("Verification: Table Row Counts")
    print("="*60)

    tables = [
        "raw_source_1",
        "raw_source_2",
        "stg_source_1",
        "stg_source_2",
        "people_canonical",
    ]

    for table in tables:
        try:
            query = f"SELECT COUNT(*) as cnt FROM `{PROJECT_ID}.{DATASET_ID}.{table}`"
            result = client.query(query).result()
            for row in result:
                print(f"  {table}: {row.cnt:,} rows")
        except Exception as e:
            print(f"  {table}: ERROR - {e}")


def verify_deduplication(client: bigquery.Client):
    """Verify no duplicates in canonical table."""
    print(f"\n{'='*60}")
    print("Verification: Deduplication Check")
    print("="*60)

    query = f"""
    SELECT linkedin_id, COUNT(*) as cnt
    FROM `{PROJECT_ID}.{DATASET_ID}.people_canonical`
    GROUP BY 1
    HAVING cnt > 1
    LIMIT 10
    """

    result = list(client.query(query).result())
    if result:
        print(f"  WARNING: Found {len(result)} duplicate linkedin_ids!")
        for row in result[:5]:
            print(f"    - {row.linkedin_id}: {row.cnt} occurrences")
    else:
        print("  OK: No duplicates found (grain = 1 row per person)")


def verify_provenance(client: bigquery.Client):
    """Verify source system distribution."""
    print(f"\n{'='*60}")
    print("Verification: Source System Distribution")
    print("="*60)

    query = f"""
    SELECT
      ARRAY_TO_STRING(provenance.source_systems, ',') as sources,
      COUNT(*) as cnt
    FROM `{PROJECT_ID}.{DATASET_ID}.people_canonical`
    GROUP BY 1
    ORDER BY 2 DESC
    """

    result = client.query(query).result()
    for row in result:
        print(f"  {row.sources}: {row.cnt:,} records")


def verify_normalization_errors(client: bigquery.Client):
    """Check for tracked normalization errors."""
    print(f"\n{'='*60}")
    print("Verification: Normalization Errors Tracked")
    print("="*60)

    query = f"""
    SELECT
      ARRAY_LENGTH(normalization_errors) as error_count,
      COUNT(*) as records
    FROM `{PROJECT_ID}.{DATASET_ID}.people_canonical`
    GROUP BY 1
    ORDER BY 1
    """

    result = client.query(query).result()
    for row in result:
        print(f"  {row.error_count} errors: {row.records:,} records")


def main():
    print("="*60)
    print("Part 3: Cleaning, Normalization & Merge Logic Pipeline")
    print("="*60)

    # Initialize client
    print(f"\nConnecting to BigQuery project: {PROJECT_ID}")
    client = bigquery.Client(project=PROJECT_ID)

    # Verify dataset exists
    try:
        client.get_dataset(DATASET_ID)
        print(f"Dataset {DATASET_ID} exists")
    except Exception:
        print(f"Creating dataset {DATASET_ID}...")
        dataset = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
        dataset.location = "US"
        client.create_dataset(dataset)
        print(f"Dataset {DATASET_ID} created")

    # Step 1: Create external table for Source 1
    result = run_sql_file(
        client,
        SQL_DIR / "01_create_external_tables.sql",
        "Create External Tables (Bronze - Source 1 only)"
    )
    all_success = result["success"]

    # Step 2: Verify Source 2 was loaded via bash script
    result = check_source_2_loaded(client)
    if not result["success"]:
        all_success = False
        if "--fail-fast" in sys.argv:
            print("Stopping due to --fail-fast flag")
            sys.exit(1)

    # Remaining pipeline steps
    steps = [
        ("02_validate_sources.sql", "Validate Source Data"),
        ("03_staging_source_1.sql", "Stage Source 1 (Silver)"),
        ("04_staging_source_2.sql", "Stage Source 2 (Silver)"),
        ("05_merge_canonical.sql", "Merge to Canonical (Gold)"),
        ("06_derived_fields.sql", "Compute Derived Fields"),
        ("07_firestore_export_view.sql", "Create Firestore Export View"),
    ]

    # Execute remaining pipeline steps
    for sql_file, description in steps:
        sql_path = SQL_DIR / sql_file
        if not sql_path.exists():
            print(f"ERROR: SQL file not found: {sql_path}")
            all_success = False
            continue

        result = run_sql_file(client, sql_path, description)
        if not result["success"]:
            all_success = False
            print(f"FAILED: {description}")
            if "--fail-fast" in sys.argv:
                print("Stopping due to --fail-fast flag")
                sys.exit(1)

    # Verification
    print("\n" + "="*60)
    print("Running Verification Checks")
    print("="*60)

    verify_table_counts(client)
    verify_deduplication(client)
    verify_provenance(client)
    verify_normalization_errors(client)

    # Summary
    print("\n" + "="*60)
    print("Pipeline Summary")
    print("="*60)
    if all_success:
        print("STATUS: All steps completed successfully")
    else:
        print("STATUS: Some steps had errors (check logs above)")

    return 0 if all_success else 1


if __name__ == "__main__":
    sys.exit(main())
