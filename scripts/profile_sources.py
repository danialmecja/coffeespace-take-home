#!/usr/bin/env python3
"""
Data Profiling Script for CoffeeSpace Take-Home

Industry-standard data profiling approach:
1. Sample meaningful volume (not just 1-2 records)
2. Compute statistical metrics with evidence
3. Distinguish schema observations from quality claims

Usage:
    uv run python scripts/profile_sources.py
"""

import json
import subprocess
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
import random

# Configuration
SOURCE_1_URI = "gs://coffeespace-sandbox-source-1/CoffeeSpaceTestDatav4.jsonl"
SOURCE_2_URI = "gs://coffeespace-sandbox-source-2/"
SAMPLE_SIZE = 10000  # Records per source
OUTPUT_DIR = Path("docs/part-1-data-profiling")


def stream_jsonl_from_gcs(uri: str, limit: int) -> list[dict]:
    """Stream JSONL from GCS, return first N records."""
    print(f"Streaming {limit} records from {uri}...")

    # Use gsutil cat with head to avoid downloading 11GB
    cmd = f"gsutil cat {uri} | head -n {limit}"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)

    if result.returncode != 0:
        print(f"Error: {result.stderr}")
        return []

    records = []
    for line in result.stdout.strip().split('\n'):
        if line:
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError as e:
                print(f"JSON parse error: {e}")

    print(f"  Loaded {len(records)} records from Source 1")
    return records


def sample_json_files_from_gcs(base_uri: str, sample_files: int, records_per_file: int) -> list[dict]:
    """Sample records from multiple JSON files in GCS."""
    print(f"Listing files in {base_uri}...")

    # List all files
    result = subprocess.run(
        f"gsutil ls {base_uri}",
        shell=True, capture_output=True, text=True
    )

    all_files = [f.strip() for f in result.stdout.strip().split('\n') if f.strip()]
    print(f"  Found {len(all_files)} files")

    # Sample files
    sampled_files = random.sample(all_files, min(sample_files, len(all_files)))
    print(f"  Sampling from {len(sampled_files)} files...")

    records = []
    for i, file_uri in enumerate(sampled_files):
        if i % 10 == 0:
            print(f"    Processing file {i+1}/{len(sampled_files)}...")

        result = subprocess.run(
            f"gsutil cat {file_uri}",
            shell=True, capture_output=True, text=True
        )

        if result.returncode != 0:
            print(f"  Error reading {file_uri}: {result.stderr}")
            continue

        try:
            data = json.loads(result.stdout)
            # Handle both array of records and single record
            if isinstance(data, list):
                # Sample from this file's records
                sampled = random.sample(data, min(records_per_file, len(data)))
                records.extend(sampled)
            else:
                records.append(data)
        except json.JSONDecodeError as e:
            print(f"  JSON parse error in {file_uri}: {e}")

    print(f"  Loaded {len(records)} total records from Source 2")
    return records


class DataProfiler:
    """Compute statistical profiles for a list of records."""

    def __init__(self, records: list[dict], source_name: str):
        self.records = records
        self.source_name = source_name
        self.total_count = len(records)
        self.field_stats = defaultdict(lambda: {
            'present_count': 0,
            'null_count': 0,
            'empty_count': 0,
            'type_counts': defaultdict(int),
            'sample_values': [],
            'value_lengths': [],
            'numeric_values': [],
        })

    def profile(self):
        """Run profiling on all records."""
        print(f"\nProfiling {self.source_name} ({self.total_count} records)...")

        for record in self.records:
            self._profile_record(record, prefix='')

        return self._compute_summary()

    def _profile_record(self, obj, prefix: str):
        """Recursively profile a record."""
        if isinstance(obj, dict):
            for key, value in obj.items():
                field_path = f"{prefix}.{key}" if prefix else key
                self._record_field_stats(field_path, value)

                # Recurse into nested structures (limit depth to avoid explosion)
                if prefix.count('.') < 2:
                    if isinstance(value, dict):
                        self._profile_record(value, field_path)
                    elif isinstance(value, list) and value and isinstance(value[0], dict):
                        self._profile_record(value[0], f"{field_path}[]")

    def _record_field_stats(self, field_path: str, value):
        """Record statistics for a single field value."""
        stats = self.field_stats[field_path]
        stats['present_count'] += 1

        type_name = type(value).__name__
        stats['type_counts'][type_name] += 1

        if value is None:
            stats['null_count'] += 1
        elif value == '' or value == [] or value == {}:
            stats['empty_count'] += 1
        else:
            if len(stats['sample_values']) < 5:
                if isinstance(value, str) and len(value) > 100:
                    stats['sample_values'].append(value[:100] + '...')
                elif isinstance(value, (list, dict)) and len(str(value)) > 100:
                    stats['sample_values'].append(f"{type_name}[{len(value)} items]")
                else:
                    stats['sample_values'].append(value)

            if isinstance(value, (str, list)):
                stats['value_lengths'].append(len(value))

            if isinstance(value, (int, float)) and not isinstance(value, bool):
                stats['numeric_values'].append(value)

    def _compute_summary(self) -> dict:
        """Compute summary statistics."""
        summary = {
            'source': self.source_name,
            'total_records': self.total_count,
            'fields': {}
        }

        for field_path, stats in sorted(self.field_stats.items()):
            present_rate = stats['present_count'] / self.total_count if self.total_count > 0 else 0
            null_rate = stats['null_count'] / stats['present_count'] if stats['present_count'] > 0 else 0
            empty_rate = stats['empty_count'] / stats['present_count'] if stats['present_count'] > 0 else 0

            field_summary = {
                'present_rate': round(present_rate, 4),
                'null_rate': round(null_rate, 4),
                'empty_rate': round(empty_rate, 4),
                'completeness': round(1 - null_rate - empty_rate, 4),
                'types': dict(stats['type_counts']),
                'sample_values': stats['sample_values'][:3],
            }

            if stats['value_lengths']:
                lengths = stats['value_lengths']
                field_summary['length_stats'] = {
                    'min': min(lengths),
                    'max': max(lengths),
                    'avg': round(sum(lengths) / len(lengths), 2)
                }

            if stats['numeric_values']:
                nums = stats['numeric_values']
                field_summary['numeric_stats'] = {
                    'min': min(nums),
                    'max': max(nums),
                    'avg': round(sum(nums) / len(nums), 2)
                }

            summary['fields'][field_path] = field_summary

        return summary


def identify_quality_issues(profile: dict) -> list[dict]:
    """Identify data quality issues from profile statistics."""
    issues = []

    for field_path, stats in profile['fields'].items():
        # Skip nested fields for top-level issue summary
        if field_path.count('.') > 1:
            continue

        if stats['null_rate'] > 0.5:
            issues.append({
                'field': field_path,
                'issue': 'HIGH_NULL_RATE',
                'severity': 'MEDIUM' if stats['null_rate'] < 0.9 else 'HIGH',
                'metric': f"{stats['null_rate']*100:.1f}% null",
                'sample_size': profile['total_records']
            })

        if stats['empty_rate'] > 0.3:
            issues.append({
                'field': field_path,
                'issue': 'HIGH_EMPTY_RATE',
                'severity': 'MEDIUM',
                'metric': f"{stats['empty_rate']*100:.1f}% empty",
                'sample_size': profile['total_records']
            })

        if len(stats['types']) > 1 and 'NoneType' not in stats['types']:
            issues.append({
                'field': field_path,
                'issue': 'TYPE_INCONSISTENCY',
                'severity': 'HIGH',
                'metric': f"Mixed types: {list(stats['types'].keys())}",
                'sample_size': profile['total_records']
            })

        if stats['present_rate'] < 0.5:
            issues.append({
                'field': field_path,
                'issue': 'SPARSE_FIELD',
                'severity': 'LOW',
                'metric': f"Only present in {stats['present_rate']*100:.1f}% of records",
                'sample_size': profile['total_records']
            })

    severity_order = {'HIGH': 0, 'MEDIUM': 1, 'LOW': 2}
    issues.sort(key=lambda x: severity_order.get(x['severity'], 3))

    return issues


def generate_markdown_report(profile1: dict, profile2: dict, issues1: list, issues2: list) -> str:
    """Generate markdown report from profiles."""

    report = f"""# Data Profiling Report

**Generated**: {datetime.now().isoformat()}
**Methodology**: Statistical sampling from GCS sources

## Executive Summary

| Metric | Source 1 (Aviato) | Source 2 (LinkedIn Scraper) |
|--------|-------------------|------------------------------|
| **Records Sampled** | {profile1['total_records']:,} | {profile2['total_records']:,} |
| **Top-Level Fields** | {len([f for f in profile1['fields'] if '.' not in f])} | {len([f for f in profile2['fields'] if '.' not in f])} |
| **Quality Issues Found** | {len(issues1)} | {len(issues2)} |

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
"""

    for field, stats in sorted(profile1['fields'].items()):
        if '.' not in field:
            report += f"| `{field}` | {stats['present_rate']*100:.0f}% | {stats['null_rate']*100:.1f}% | {stats['empty_rate']*100:.1f}% | {stats['completeness']*100:.1f}% |\n"

    report += f"""
### Data Quality Issues (Top 5)

| Rank | Field | Issue | Severity | Evidence |
|------|-------|-------|----------|----------|
"""

    for i, issue in enumerate(issues1[:5], 1):
        report += f"| {i} | `{issue['field']}` | {issue['issue']} | {issue['severity']} | {issue['metric']} (n={issue['sample_size']:,}) |\n"

    if not issues1:
        report += "| - | No significant issues found | - | - | - |\n"

    report += f"""
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
"""

    for field, stats in sorted(profile2['fields'].items()):
        if '.' not in field:
            report += f"| `{field}` | {stats['present_rate']*100:.0f}% | {stats['null_rate']*100:.1f}% | {stats['empty_rate']*100:.1f}% | {stats['completeness']*100:.1f}% |\n"

    report += f"""
### Data Quality Issues (Top 5)

| Rank | Field | Issue | Severity | Evidence |
|------|-------|-------|----------|----------|
"""

    for i, issue in enumerate(issues2[:5], 1):
        report += f"| {i} | `{issue['field']}` | {issue['issue']} | {issue['severity']} | {issue['metric']} (n={issue['sample_size']:,}) |\n"

    if not issues2:
        report += "| - | No significant issues found | - | - | - |\n"

    report += """
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

1. **Sample Size**: {profile1['total_records']:,} records from Source 1, {profile2['total_records']:,} from Source 2
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
"""

    return report


def main():
    print("=" * 60)
    print("CoffeeSpace Data Profiling")
    print("=" * 60)

    source1_records = stream_jsonl_from_gcs(SOURCE_1_URI, SAMPLE_SIZE)
    source2_records = sample_json_files_from_gcs(
        SOURCE_2_URI,
        sample_files=50,
        records_per_file=200
    )

    if not source1_records:
        print("ERROR: Failed to load Source 1 data")
        sys.exit(1)

    if not source2_records:
        print("ERROR: Failed to load Source 2 data")
        sys.exit(1)

    profiler1 = DataProfiler(source1_records, "Source 1 (Aviato)")
    profile1 = profiler1.profile()

    profiler2 = DataProfiler(source2_records, "Source 2 (LinkedIn Scraper)")
    profile2 = profiler2.profile()

    issues1 = identify_quality_issues(profile1)
    issues2 = identify_quality_issues(profile2)

    print(f"\nSource 1 quality issues: {len(issues1)}")
    for issue in issues1[:3]:
        print(f"  - {issue['field']}: {issue['issue']} ({issue['metric']})")

    print(f"\nSource 2 quality issues: {len(issues2)}")
    for issue in issues2[:3]:
        print(f"  - {issue['field']}: {issue['issue']} ({issue['metric']})")

    report = generate_markdown_report(profile1, profile2, issues1, issues2)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    report_path = OUTPUT_DIR / "data-profiling-report.md"
    with open(report_path, 'w') as f:
        f.write(report)
    print(f"\n✓ Report saved to: {report_path}")

    profiles_path = OUTPUT_DIR / "profiles-raw.json"
    with open(profiles_path, 'w') as f:
        json.dump({
            'source1': profile1,
            'source2': profile2,
            'issues1': issues1,
            'issues2': issues2
        }, f, indent=2, default=str)
    print(f"✓ Raw profiles saved to: {profiles_path}")

    print("\n" + "=" * 60)
    print("Profiling complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
