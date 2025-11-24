# BigQuery Metadata Tables

This document describes the BigQuery external tables and views for analyzing GCS file metadata.

## Prerequisites

1. Upload the CSV files to GCS:

```bash
gsutil cp .scratch/test-jarvus-transit-data-demo-raw-files.csv gs://transit-data-demo-metadata/
gsutil cp .scratch/jarvus-transit-data-demo-raw-files.csv gs://transit-data-demo-metadata/
```

2. Create the tables by running `create-metadata-tables.sql` in BigQuery:
   - Replace `{PROJECT_ID}` with your GCP project ID
   - Replace `{DATASET}` with your BigQuery dataset name

## Tables

### External Tables

#### `test_raw_files_metadata`

Metadata for all files in `test-jarvus-transit-data-demo-raw` bucket.

#### `prod_raw_files_metadata`

Metadata for all files in `jarvus-transit-data-demo-raw` bucket.

**Schema:**

- `feed_type` (STRING): Feed type (e.g., gtfs_rt__vehicle_positions, septa__arrivals)
- `timestamp` (TIMESTAMP): Timestamp from the ts= partition in the file path
- `feed_url` (STRING): Decoded feed URL (base64-decoded from filename)
- `file_size_bytes` (INT64): File size in bytes
- `created_time` (TIMESTAMP): GCS file creation timestamp

## Views

### `all_raw_files_metadata`

Union of test and prod metadata with an `environment` column.

### `file_collection_summary`

High-level summary statistics by environment and feed type:

- Total files and storage size
- Date ranges (earliest/latest)
- Average file size
- Unique feed URLs
- Days of data collected

### `daily_collection_stats`

Daily collection statistics showing:

- Files collected per day
- Total storage used
- Unique feeds
- Collection time spans

### `hourly_collection_patterns`

Analysis of collection patterns by hour of day:

- Files collected per hour
- Average file sizes
- Days observed

### `feed_url_analysis`

Per-URL statistics:

- Total fetches
- Storage used
- Average file size
- First/last fetch times
- Days between collections

### `collection_gaps`

Identifies missing hours in data collection to help spot outages or gaps.

### `file_size_trends`

File size trends over time with:

- Min/max/average/median sizes
- Standard deviation
- Files per day

### `test_vs_prod_comparison`

Side-by-side comparison of test vs prod buckets by feed type.

## Example Queries

### Find the most actively collected feeds

```sql
SELECT
  feed_type,
  environment,
  total_files,
  total_size_gb,
  unique_feed_urls,
  days_collected,
  ROUND(total_files / days_collected, 1) as avg_files_per_day
FROM `{PROJECT_ID}.{DATASET}.file_collection_summary`
ORDER BY total_files DESC
LIMIT 10;
```

### Identify collection gaps in the last 7 days

```sql
SELECT
  environment,
  feed_type,
  missing_date,
  COUNT(*) as missing_hours
FROM `{PROJECT_ID}.{DATASET}.collection_gaps`
WHERE missing_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
GROUP BY environment, feed_type, missing_date
HAVING missing_hours > 3  -- More than 3 hours missing
ORDER BY missing_date DESC, missing_hours DESC;
```

### Compare file sizes for the same feed URL over time

```sql
SELECT
  feed_url,
  collection_date,
  files_per_day,
  ROUND(avg_size / 1024, 2) as avg_size_kb,
  ROUND(min_size / 1024, 2) as min_size_kb,
  ROUND(max_size / 1024, 2) as max_size_kb
FROM `{PROJECT_ID}.{DATASET}.file_size_trends`
WHERE feed_type = 'gtfs_rt__vehicle_positions'
  AND feed_url LIKE '%septa%'
  AND collection_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
ORDER BY collection_date DESC;
```

### Find feeds that stopped being collected

```sql
WITH recent_activity AS (
  SELECT
    environment,
    feed_type,
    feed_url,
    MAX(timestamp) as last_fetch,
    COUNT(*) as total_fetches
  FROM `{PROJECT_ID}.{DATASET}.all_raw_files_metadata`
  GROUP BY environment, feed_type, feed_url
)
SELECT
  environment,
  feed_type,
  feed_url,
  last_fetch,
  TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), last_fetch, DAY) as days_since_last_fetch,
  total_fetches
FROM recent_activity
WHERE TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), last_fetch, DAY) > 7
ORDER BY days_since_last_fetch DESC;
```

### Analyze collection consistency by feed type

```sql
WITH daily_counts AS (
  SELECT
    environment,
    feed_type,
    DATE(timestamp) as date,
    COUNT(*) as files
  FROM `{PROJECT_ID}.{DATASET}.all_raw_files_metadata`
  WHERE DATE(timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  GROUP BY environment, feed_type, date
)
SELECT
  environment,
  feed_type,
  COUNT(DISTINCT date) as days_with_data,
  ROUND(AVG(files), 1) as avg_files_per_day,
  MIN(files) as min_files,
  MAX(files) as max_files,
  ROUND(STDDEV(files), 1) as stddev_files,
  -- Coefficient of variation (stddev/mean) - lower is more consistent
  ROUND(STDDEV(files) / AVG(files), 2) as consistency_score
FROM daily_counts
GROUP BY environment, feed_type
ORDER BY consistency_score;
```

### Calculate storage costs

```sql
-- Assuming $0.020 per GB per month for Standard storage
SELECT
  environment,
  feed_type,
  total_size_gb,
  ROUND(total_size_gb * 0.020, 2) as estimated_monthly_storage_cost_usd,
  days_collected,
  ROUND((total_size_gb / days_collected) * 365, 2) as projected_annual_size_gb,
  ROUND((total_size_gb / days_collected) * 365 * 0.020, 2) as projected_annual_cost_usd
FROM `{PROJECT_ID}.{DATASET}.file_collection_summary`
ORDER BY total_size_gb DESC;
```

### Find duplicate collections (same feed at same time)

```sql
SELECT
  environment,
  feed_type,
  feed_url,
  timestamp,
  COUNT(*) as duplicate_count,
  SUM(file_size_bytes) as total_duplicate_bytes
FROM `{PROJECT_ID}.{DATASET}.all_raw_files_metadata`
GROUP BY environment, feed_type, feed_url, timestamp
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC, total_duplicate_bytes DESC;
```

### Peak collection times by feed

```sql
SELECT
  feed_type,
  hour_of_day,
  SUM(total_files) as files_across_all_days,
  ROUND(AVG(avg_file_size) / 1024, 2) as avg_size_kb
FROM `{PROJECT_ID}.{DATASET}.hourly_collection_patterns`
WHERE environment = 'prod'
GROUP BY feed_type, hour_of_day
ORDER BY feed_type, hour_of_day;
```

### Test vs Prod discrepancies

```sql
SELECT
  feed_type,
  count_diff,
  ROUND(test_size_gb - prod_size_gb, 2) as size_diff_gb,
  test_days - prod_days as days_diff,
  test_unique_urls - prod_unique_urls as url_diff
FROM `{PROJECT_ID}.{DATASET}.test_vs_prod_comparison`
WHERE ABS(count_diff) > 100  -- More than 100 files difference
   OR ABS(test_size_gb - prod_size_gb) > 0.1  -- More than 100MB difference
ORDER BY ABS(count_diff) DESC;
```

## Parsed Buckets Metadata

### External Tables

#### `test_parsed_files_metadata`

Metadata for all files in `test-jarvus-transit-data-demo-parsed` bucket.

#### `prod_parsed_files_metadata`

Metadata for all files in `jarvus-transit-data-demo-parsed` bucket.

**Schema:**

- `table_name` (STRING): Table name (e.g., gtfs_rt__vehicle_positions, gtfs_schedule__stops_txt)
- `date` (DATE): Date from the dt= partition
- `hour` (TIMESTAMP): Hour from the hour= partition
- `feed_url` (STRING): Decoded feed URL (base64-decoded from filename)
- `file_path` (STRING): Full gs:// path
- `file_size_bytes` (INT64): File size in bytes
- `created_time` (TIMESTAMP): GCS file creation timestamp
- `file_type` (STRING): "data" for .jsonl.gz files, "outcomes" for .jsonl files, "other" otherwise

### Parsed Bucket Views

#### `all_parsed_files_metadata`

Union of test and prod parsed metadata with an `environment` column.

#### `parsed_file_collection_summary`

High-level summary statistics by environment, table, and file type:

- Total files and storage size
- Hour ranges (earliest/latest)
- Average file size
- Unique feed URLs
- Days and hours of data collected

#### `parsed_daily_stats`

Daily statistics for parsed files:

- Files collected per day
- Total storage used
- Unique feeds
- Hours with data

#### `raw_to_parsed_comparison`

Compares raw and parsed files hourly:

- Raw file counts and sizes
- Parsed file counts and sizes
- Compression ratios
- Outcome file counts

#### `parsed_table_types`

Analyzes parsed tables by data source (GTFS RT, GTFS Schedule, SEPTA):

- Base table names
- Whether outcomes exist
- Table variants
- Total files and storage

#### `parsed_outcomes_summary`

Summary of parsing outcomes files:

- Outcome file counts
- Hour ranges
- Total storage

#### `test_vs_prod_parsed_comparison`

Side-by-side comparison of test vs prod parsed buckets by table and file type.

## Example Queries for Parsed Data

### Analyze compression ratios

```sql
SELECT
  environment,
  feed_type,
  table_name,
  date,
  SUM(raw_files) as total_raw_files,
  ROUND(SUM(raw_mb), 2) as total_raw_mb,
  SUM(parsed_data_files) as total_parsed_files,
  ROUND(SUM(parsed_data_mb), 2) as total_parsed_mb,
  ROUND(AVG(compression_pct), 2) as avg_compression_pct
FROM `{PROJECT_ID}.{DATASET}.raw_to_parsed_comparison`
WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
GROUP BY environment, feed_type, table_name, date
ORDER BY date DESC, environment, feed_type;
```

### Find parsed tables without outcomes

```sql
WITH outcomes AS (
  SELECT DISTINCT
    environment,
    REGEXP_EXTRACT(table_name, r'^(.+)__outcomes$', 1) as base_table
  FROM `{PROJECT_ID}.{DATASET}.all_parsed_files_metadata`
  WHERE file_type = 'outcomes'
),
data_tables AS (
  SELECT DISTINCT
    environment,
    table_name
  FROM `{PROJECT_ID}.{DATASET}.all_parsed_files_metadata`
  WHERE file_type = 'data'
    AND table_name NOT LIKE '%__outcomes'
)
SELECT
  d.environment,
  d.table_name,
  CASE WHEN o.base_table IS NULL THEN 'No outcomes' ELSE 'Has outcomes' END as status
FROM data_tables d
LEFT JOIN outcomes o
  ON d.environment = o.environment
  AND d.table_name = o.base_table
ORDER BY d.environment, status, d.table_name;
```

### Calculate hourly parsing throughput

```sql
SELECT
  environment,
  table_name,
  DATE_TRUNC(hour, HOUR) as hour,
  COUNT(*) as files_parsed,
  SUM(file_size_bytes) / POW(1024, 2) as mb_parsed,
  COUNT(DISTINCT feed_url) as unique_sources
FROM `{PROJECT_ID}.{DATASET}.all_parsed_files_metadata`
WHERE file_type = 'data'
  AND DATE(hour) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
GROUP BY environment, table_name, hour
ORDER BY hour DESC, environment, table_name;
```

### Identify largest parsed tables

```sql
SELECT
  environment,
  table_name,
  file_type,
  total_files,
  total_size_gb,
  days_collected,
  hours_collected,
  ROUND(total_size_gb / days_collected, 2) as gb_per_day,
  ROUND(total_files / days_collected, 1) as files_per_day,
  ROUND(avg_file_size_bytes / POW(1024, 2), 2) as avg_file_size_mb
FROM `{PROJECT_ID}.{DATASET}.parsed_file_collection_summary`
WHERE file_type = 'data'
ORDER BY total_size_gb DESC
LIMIT 20;
```

### Compare parsed data freshness

```sql
SELECT
  environment,
  table_name,
  MAX(hour) as latest_hour,
  MAX(created_time) as latest_created,
  TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(hour), HOUR) as hours_behind,
  COUNT(DISTINCT DATE(hour)) as days_with_data
FROM `{PROJECT_ID}.{DATASET}.all_parsed_files_metadata`
WHERE file_type = 'data'
GROUP BY environment, table_name
HAVING hours_behind > 24  -- More than 24 hours old
ORDER BY hours_behind DESC;
```

### Analyze GTFS Schedule subtable sizes

```sql
SELECT
  environment,
  table_name,
  total_files,
  total_size_mb,
  avg_file_size_bytes / 1024 as avg_file_size_kb,
  days_collected,
  ROUND(total_files / days_collected, 1) as files_per_day
FROM `{PROJECT_ID}.{DATASET}.parsed_file_collection_summary`
WHERE table_name LIKE 'gtfs_schedule__%'
  AND file_type = 'data'
ORDER BY environment, total_size_mb DESC;
```

### Find parsing gaps by comparing raw and parsed

```sql
WITH raw_hours AS (
  SELECT DISTINCT
    environment,
    feed_type,
    DATE(timestamp) as date,
    TIMESTAMP_TRUNC(timestamp, HOUR) as hour
  FROM `{PROJECT_ID}.{DATASET}.all_raw_files_metadata`
  WHERE DATE(timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
),
parsed_hours AS (
  SELECT DISTINCT
    environment,
    REGEXP_EXTRACT(table_name, r'^([^_]+(?:_[^_]+)*?)(?:__.+)?$', 1) as feed_type,
    date,
    TIMESTAMP_TRUNC(hour, HOUR) as hour
  FROM `{PROJECT_ID}.{DATASET}.all_parsed_files_metadata`
  WHERE file_type = 'data'
    AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
)
SELECT
  r.environment,
  r.feed_type,
  r.date,
  r.hour as missing_hour,
  'Raw exists but not parsed' as status
FROM raw_hours r
LEFT JOIN parsed_hours p
  ON r.environment = p.environment
  AND r.feed_type = p.feed_type
  AND r.date = p.date
  AND r.hour = p.hour
WHERE p.hour IS NULL
ORDER BY r.environment, r.feed_type, r.hour DESC
LIMIT 100;
```

## Bucket Coverage Summary Tables

### Overview

Instead of listing every individual file (which can be 50K+ rows), the coverage summary tables provide an aggregated view showing:

- Date and time ranges for each feed
- File counts and storage sizes
- Gaps in data collection
- Completeness metrics

### External Tables

#### Coverage Summary Tables

- `test_raw_bucket_coverage` - Coverage for test raw bucket
- `prod_raw_bucket_coverage` - Coverage for prod raw bucket
- `test_parsed_bucket_coverage` - Coverage for test parsed bucket
- `prod_parsed_bucket_coverage` - Coverage for prod parsed bucket

**Schema:**

- `environment` (STRING): test/prod
- `bucket` (STRING): GCS bucket name
- `bucket_type` (STRING): raw/parsed
- `feed_type` (STRING): Feed type or table name
- `feed_url` (STRING): Decoded feed URL
- `first_date` (DATE): First date with data
- `last_date` (DATE): Last date with data
- `first_timestamp` (TIMESTAMP): First timestamp
- `last_timestamp` (TIMESTAMP): Last timestamp
- `total_files` (INT64): Total number of files
- `total_size_bytes` (INT64): Total size in bytes
- `total_size_gb` (FLOAT64): Total size in GB
- `days_covered` (INT64): Number of days with data
- `hours_covered` (INT64): Number of hours with data
- `has_gaps` (BOOL): Whether there are gaps in hourly coverage
- `gap_count` (INT64): Number of missing hours

### Coverage Analysis Views

#### `all_bucket_coverage`

Union of all coverage tables (4 buckets).

#### `coverage_by_feed_type`

Aggregates coverage by feed type across all environments and bucket types.

#### `data_completeness`

Calculates completeness percentage and categorizes feeds as:

- Complete (no gaps)
- Mostly Complete (< 10% gaps)
- Partial (10-50% gaps)
- Sparse (> 50% gaps)

#### `largest_feeds`

Top 50 feeds by storage size, with per-day metrics.

### Generating Coverage Summaries

The coverage summaries are generated using Python scripts instead of listing every file:

```bash
# Analyze all buckets
./scripts/analyze-all-bucket-coverage

# Analyze specific bucket
python3 scripts/analyze-bucket-coverage.py test-jarvus-transit-data-demo-raw

# Test with limited files
./scripts/analyze-all-bucket-coverage --limit 10000
```

Output files in `.scratch/`:

- `test-jarvus-transit-data-demo-raw-coverage.csv`
- `jarvus-transit-data-demo-raw-coverage.csv`
- `test-jarvus-transit-data-demo-parsed-coverage.csv`
- `jarvus-transit-data-demo-parsed-coverage.csv`

Upload these to `gs://transit-data-demo-metadata/` for BigQuery access.

### Example Queries for Coverage Analysis

#### View all feeds with their coverage ranges

```sql
SELECT
  environment,
  bucket_type,
  feed_type,
  feed_url,
  first_date,
  last_date,
  DATE_DIFF(last_date, first_date, DAY) as date_span_days,
  total_files,
  total_size_gb,
  days_covered,
  hours_covered,
  gap_count,
  ROUND(hours_covered / NULLIF(days_covered * 24, 0) * 100, 1) as hourly_completeness_pct
FROM `{PROJECT_ID}.{DATASET}.all_bucket_coverage`
ORDER BY total_size_gb DESC;
```

#### Find feeds with significant gaps

```sql
SELECT
  environment,
  bucket_type,
  feed_type,
  feed_url,
  days_covered,
  hours_covered,
  gap_count,
  ROUND(gap_count / NULLIF(hours_covered + gap_count, 0) * 100, 1) as gap_percentage
FROM `{PROJECT_ID}.{DATASET}.all_bucket_coverage`
WHERE has_gaps = TRUE
  AND gap_count > 100  -- More than 100 missing hours
ORDER BY gap_count DESC;
```

#### Compare test vs prod coverage

```sql
WITH coverage_pivot AS (
  SELECT
    feed_type,
    feed_url,
    MAX(CASE WHEN environment = 'test' THEN total_files END) as test_files,
    MAX(CASE WHEN environment = 'test' THEN days_covered END) as test_days,
    MAX(CASE WHEN environment = 'prod' THEN total_files END) as prod_files,
    MAX(CASE WHEN environment = 'prod' THEN days_covered END) as prod_days,
    MAX(CASE WHEN environment = 'test' THEN total_size_gb END) as test_gb,
    MAX(CASE WHEN environment = 'prod' THEN total_size_gb END) as prod_gb
  FROM `{PROJECT_ID}.{DATASET}.all_bucket_coverage`
  WHERE bucket_type = 'raw'
  GROUP BY feed_type, feed_url
)
SELECT
  feed_type,
  feed_url,
  test_files,
  prod_files,
  test_files - IFNULL(prod_files, 0) as file_diff,
  test_days,
  prod_days,
  test_gb,
  prod_gb
FROM coverage_pivot
ORDER BY ABS(test_files - IFNULL(prod_files, 0)) DESC;
```

#### Identify stale feeds (no recent data)

```sql
SELECT
  environment,
  bucket_type,
  feed_type,
  feed_url,
  last_date,
  last_timestamp,
  DATE_DIFF(CURRENT_DATE(), last_date, DAY) as days_since_last_data,
  total_files,
  total_size_gb
FROM `{PROJECT_ID}.{DATASET}.all_bucket_coverage`
WHERE DATE_DIFF(CURRENT_DATE(), last_date, DAY) > 7  -- No data in last week
ORDER BY days_since_last_data DESC;
```

#### Calculate storage costs by feed

```sql
-- Assuming $0.020 per GB per month for Standard storage
SELECT
  environment,
  bucket_type,
  feed_type,
  feed_url,
  total_size_gb,
  ROUND(total_size_gb * 0.020, 4) as monthly_storage_cost_usd,
  days_covered,
  ROUND((total_size_gb / days_covered) * 365, 2) as projected_annual_gb,
  ROUND((total_size_gb / days_covered) * 365 * 0.020, 2) as projected_annual_cost_usd
FROM `{PROJECT_ID}.{DATASET}.all_bucket_coverage`
WHERE days_covered > 0
ORDER BY projected_annual_cost_usd DESC
LIMIT 20;
```

#### Find feeds with unusual file sizes

```sql
SELECT
  environment,
  bucket_type,
  feed_type,
  feed_url,
  total_files,
  total_size_gb,
  ROUND((total_size_gb * 1024 * 1024) / total_files, 2) as avg_file_size_mb,
  days_covered
FROM `{PROJECT_ID}.{DATASET}.all_bucket_coverage`
WHERE total_files > 0
ORDER BY avg_file_size_mb DESC
LIMIT 20;
```

#### Analyze hourly collection consistency

```sql
SELECT
  environment,
  bucket_type,
  feed_type,
  COUNT(DISTINCT feed_url) as unique_urls,
  AVG(hours_covered) as avg_hours_per_url,
  AVG(days_covered) as avg_days_per_url,
  AVG(hours_covered / NULLIF(days_covered, 0)) as avg_hours_per_day,
  COUNTIF(has_gaps) as urls_with_gaps,
  AVG(gap_count) as avg_gaps
FROM `{PROJECT_ID}.{DATASET}.all_bucket_coverage`
GROUP BY environment, bucket_type, feed_type
ORDER BY environment, bucket_type, feed_type;
```
