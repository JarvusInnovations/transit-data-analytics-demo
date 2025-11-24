#!/usr/bin/env python3
"""
Analyze GCS bucket coverage for transit data.

Efficiently aggregates file metadata to identify date/time ranges and gaps
for each feed without listing every individual file.

Usage:
    python analyze-bucket-coverage.py test-jarvus-transit-data-demo-raw --output coverage.csv
    python analyze-bucket-coverage.py --bucket jarvus-transit-data-demo-parsed --bucket-type parsed
"""

import argparse
import base64
import csv
import re
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, Set, Tuple, Optional

from google.cloud import storage


def decode_base64url(encoded: str) -> str:
    """Decode base64url-encoded string."""
    # base64url uses - instead of + and _ instead of /
    std_base64 = encoded.replace('-', '+').replace('_', '/')

    # Add padding if needed
    padding = 4 - len(std_base64) % 4
    if padding != 4:
        std_base64 += '=' * padding

    try:
        return base64.b64decode(std_base64).decode('utf-8')
    except Exception:
        return ''


def parse_raw_bucket_path(path: str) -> Optional[Tuple[str, str, str, str, str]]:
    """
    Parse raw bucket Hive-partitioned path.

    Format: feed_type/dt=DATE/hour=HOUR/ts=TIMESTAMP/base64url=BASE/ENCODED.json
    Returns: (feed_type, date, hour, timestamp, base64_encoded_url) or None
    """
    pattern = r'^([^/]+)/dt=([^/]+)/hour=([^/]+)/ts=([^/]+)/base64url=[^/]+/([^/]+)\.json$'
    match = re.match(pattern, path)

    if match:
        feed_type, date, hour, timestamp, base64_encoded_url = match.groups()
        return (feed_type, date, hour, timestamp, base64_encoded_url)

    return None


def parse_parsed_bucket_path(path: str) -> Optional[Tuple[str, str, str, str]]:
    """
    Parse parsed bucket Hive-partitioned path.

    Format: table/dt=DATE/hour=HOUR/base64url.jsonl.gz
    Returns: (table_name, date, hour, base64_encoded_url) or None
    """
    pattern = r'^([^/]+)/dt=([^/]+)/hour=([^/]+)/([^/]+)\.(jsonl\.gz|jsonl)$'
    match = re.match(pattern, path)

    if match:
        table_name, date, hour, base64_encoded_url, _ = match.groups()
        # Don't decode if it looks like a timestamp (outcomes files)
        if re.match(r'^\d{4}-\d{2}-\d{2}T', base64_encoded_url):
            return None
        return (table_name, date, hour, base64_encoded_url)

    return None


def analyze_bucket(
    bucket_name: str,
    bucket_type: str = 'raw',
    environment: str = 'unknown',
    limit: Optional[int] = None
) -> Dict[Tuple[str, str], Dict]:
    """
    Analyze GCS bucket and aggregate coverage by feed.

    Args:
        bucket_name: GCS bucket name (with or without gs:// prefix)
        bucket_type: 'raw' or 'parsed'
        environment: 'test' or 'prod'
        limit: Optional limit on number of files to process (for testing)

    Returns:
        Dict keyed by (feed_type, base64_encoded_url) with coverage stats
    """
    # Remove gs:// prefix if present
    bucket_name = bucket_name.removeprefix('gs://')

    print(f"Analyzing {bucket_type} bucket: gs://{bucket_name}", file=sys.stderr)
    print(f"Environment: {environment}", file=sys.stderr)

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    # Aggregate data by (feed_type, base64_encoded_url)
    coverage: Dict[Tuple[str, str], Dict] = defaultdict(lambda: {
        'dates': set(),
        'hours': set(),
        'timestamps': set(),
        'file_count': 0,
        'total_size': 0,
    })

    files_processed = 0

    # Iterate through bucket with pagination
    print("Fetching file list...", file=sys.stderr)
    blobs_iterator = bucket.list_blobs(timeout=600)

    for page in blobs_iterator.pages:
        for blob in page:
            # Parse path based on bucket type
            if bucket_type == 'raw':
                parsed = parse_raw_bucket_path(blob.name)
                if parsed:
                    feed_type, date, hour, timestamp, base64_url = parsed
                else:
                    continue
            else:  # parsed
                parsed = parse_parsed_bucket_path(blob.name)
                if parsed:
                    feed_type, date, hour, base64_url = parsed
                    # For parsed buckets, use hour as timestamp
                    timestamp = hour
                else:
                    continue

            # Aggregate stats
            key = (feed_type, base64_url)
            stats = coverage[key]

            stats['dates'].add(date)
            stats['hours'].add(hour)
            stats['timestamps'].add(timestamp)
            stats['file_count'] += 1
            stats['total_size'] += blob.size or 0

            files_processed += 1

            if files_processed % 1000 == 0:
                print(f"Processed {files_processed} files...", file=sys.stderr)

            if limit and files_processed >= limit:
                print(f"Reached limit of {limit} files", file=sys.stderr)
                break

        if limit and files_processed >= limit:
            break

    print(f"Completed processing {files_processed} files", file=sys.stderr)
    print(f"Found {len(coverage)} unique feed combinations", file=sys.stderr)

    return coverage


def detect_gaps(hours: Set[str]) -> Tuple[bool, int]:
    """
    Detect gaps in hourly coverage.

    Args:
        hours: Set of hour strings (ISO 8601 format)

    Returns:
        (has_gaps, gap_count) tuple
    """
    if not hours:
        return (False, 0)

    # Parse hours to datetime objects
    try:
        hour_dts = sorted([datetime.fromisoformat(h.replace('Z', '+00:00')) for h in hours])
    except Exception:
        return (False, 0)

    if len(hour_dts) < 2:
        return (False, 0)

    # Count expected hours between first and last
    first_hour = hour_dts[0]
    last_hour = hour_dts[-1]

    hours_diff = int((last_hour - first_hour).total_seconds() / 3600)
    expected_hours = hours_diff + 1
    actual_hours = len(hour_dts)

    gap_count = expected_hours - actual_hours
    has_gaps = gap_count > 0

    return (has_gaps, gap_count)


def write_coverage_csv(
    coverage: Dict[Tuple[str, str], Dict],
    output_file: str,
    bucket_name: str,
    environment: str,
    bucket_type: str
):
    """Write coverage analysis to CSV file."""
    print(f"\nWriting results to {output_file}", file=sys.stderr)

    # Prepare rows with decoded URLs (decode each unique URL only once)
    rows = []

    for (feed_type, base64_url), stats in coverage.items():
        # Decode the base64 URL once per unique combination
        feed_url = decode_base64url(base64_url)

        # Sort timestamps and dates
        sorted_dates = sorted(stats['dates'])
        sorted_timestamps = sorted(stats['timestamps'])

        # Detect gaps
        has_gaps, gap_count = detect_gaps(stats['hours'])

        row = {
            'environment': environment,
            'bucket': bucket_name,
            'bucket_type': bucket_type,
            'feed_type': feed_type,
            'feed_url': feed_url,
            'first_date': sorted_dates[0] if sorted_dates else '',
            'last_date': sorted_dates[-1] if sorted_dates else '',
            'first_timestamp': sorted_timestamps[0] if sorted_timestamps else '',
            'last_timestamp': sorted_timestamps[-1] if sorted_timestamps else '',
            'total_files': stats['file_count'],
            'total_size_bytes': stats['total_size'],
            'total_size_gb': round(stats['total_size'] / (1024**3), 4),
            'days_covered': len(stats['dates']),
            'hours_covered': len(stats['hours']),
            'has_gaps': has_gaps,
            'gap_count': gap_count,
        }
        rows.append(row)

    # Sort by feed_type, then by file count
    rows.sort(key=lambda x: (x['feed_type'], -x['total_files']))

    # Write CSV
    if rows:
        fieldnames = rows[0].keys()

        if output_file == '-':
            writer = csv.DictWriter(sys.stdout, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        else:
            with open(output_file, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)

            print(f"✓ Wrote {len(rows)} rows to {output_file}", file=sys.stderr)
    else:
        print("No data to write", file=sys.stderr)


def infer_environment(bucket_name: str) -> str:
    """Infer environment from bucket name."""
    if 'test' in bucket_name.lower():
        return 'test'
    elif 'prod' in bucket_name.lower():
        return 'prod'
    else:
        return 'unknown'


def infer_bucket_type(bucket_name: str) -> str:
    """Infer bucket type from bucket name."""
    if 'parsed' in bucket_name.lower():
        return 'parsed'
    elif 'raw' in bucket_name.lower():
        return 'raw'
    else:
        return 'raw'  # default


def main():
    parser = argparse.ArgumentParser(
        description='Analyze GCS bucket coverage for transit data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Analyze raw bucket
  python analyze-bucket-coverage.py test-jarvus-transit-data-demo-raw

  # Analyze parsed bucket with custom output
  python analyze-bucket-coverage.py jarvus-transit-data-demo-parsed \\
    --output parsed-coverage.csv

  # Test with limited files
  python analyze-bucket-coverage.py test-jarvus-transit-data-demo-raw \\
    --limit 1000

  # Specify bucket type explicitly
  python analyze-bucket-coverage.py my-bucket \\
    --bucket-type parsed \\
    --environment prod
        """
    )

    parser.add_argument(
        'bucket',
        help='GCS bucket name (with or without gs:// prefix)'
    )
    parser.add_argument(
        '-o', '--output',
        default=None,
        help='Output CSV file (default: <bucket>-coverage.csv, use "-" for stdout)'
    )
    parser.add_argument(
        '-t', '--bucket-type',
        choices=['raw', 'parsed'],
        default=None,
        help='Bucket type (default: inferred from bucket name)'
    )
    parser.add_argument(
        '-e', '--environment',
        choices=['test', 'prod', 'unknown'],
        default=None,
        help='Environment (default: inferred from bucket name)'
    )
    parser.add_argument(
        '-l', '--limit',
        type=int,
        default=None,
        help='Limit number of files to process (for testing)'
    )

    args = parser.parse_args()

    # Clean bucket name
    bucket_name = args.bucket.removeprefix('gs://')

    # Infer parameters if not provided
    bucket_type = args.bucket_type or infer_bucket_type(bucket_name)
    environment = args.environment or infer_environment(bucket_name)

    # Determine output file
    if args.output:
        output_file = args.output
    else:
        output_file = f"{bucket_name}-coverage.csv"

    # Analyze bucket
    coverage = analyze_bucket(
        bucket_name,
        bucket_type=bucket_type,
        environment=environment,
        limit=args.limit
    )

    # Write results
    write_coverage_csv(
        coverage,
        output_file,
        bucket_name,
        environment,
        bucket_type
    )


if __name__ == '__main__':
    main()
