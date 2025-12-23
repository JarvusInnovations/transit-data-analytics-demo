-- BigQuery External Tables for GCS File Metadata Analysis
-- Assumes CSVs are uploaded to gs://transit-data-demo-metadata/

-- =============================================================================
-- External Table: Test Bucket Raw File Metadata
-- =============================================================================
CREATE OR REPLACE EXTERNAL TABLE `{PROJECT_ID}.{DATASET}.test_raw_files_metadata`
(
  feed_type STRING OPTIONS(description="Feed type (top-level folder name)"),
  timestamp TIMESTAMP OPTIONS(description="Timestamp from ts= partition"),
  feed_url STRING OPTIONS(description="Decoded feed URL from base64url filename"),
  file_size_bytes INT64 OPTIONS(description="File size in bytes"),
  created_time TIMESTAMP OPTIONS(description="GCS file creation timestamp")
)
OPTIONS (
  format = 'CSV',
  uris = ['gs://transit-data-demo-metadata/test-jarvus-transit-data-demo-raw-files.csv'],
  skip_leading_rows = 1,
  description = 'Metadata for all files in test-jarvus-transit-data-demo-raw bucket'
);

-- =============================================================================
-- External Table: Production Bucket Raw File Metadata
-- =============================================================================
CREATE OR REPLACE EXTERNAL TABLE `{PROJECT_ID}.{DATASET}.prod_raw_files_metadata`
(
  feed_type STRING OPTIONS(description="Feed type (top-level folder name)"),
  timestamp TIMESTAMP OPTIONS(description="Timestamp from ts= partition"),
  feed_url STRING OPTIONS(description="Decoded feed URL from base64url filename"),
  file_size_bytes INT64 OPTIONS(description="File size in bytes"),
  created_time TIMESTAMP OPTIONS(description="GCS file creation timestamp")
)
OPTIONS (
  format = 'CSV',
  uris = ['gs://transit-data-demo-metadata/jarvus-transit-data-demo-raw-files.csv'],
  skip_leading_rows = 1,
  description = 'Metadata for all files in jarvus-transit-data-demo-raw bucket'
);

-- =============================================================================
-- View: Combined metadata from both buckets
-- =============================================================================
CREATE OR REPLACE VIEW `{PROJECT_ID}.{DATASET}.all_raw_files_metadata` AS
SELECT
  'test' AS environment,
  *
FROM `{PROJECT_ID}.{DATASET}.test_raw_files_metadata`

UNION ALL

SELECT
  'prod' AS environment,
  *
FROM `{PROJECT_ID}.{DATASET}.prod_raw_files_metadata`;

-- =============================================================================
-- View: File collection summary by feed type
-- =============================================================================
CREATE OR REPLACE VIEW `{PROJECT_ID}.{DATASET}.file_collection_summary` AS
SELECT
  environment,
  feed_type,
  COUNT(*) as total_files,
  SUM(file_size_bytes) as total_size_bytes,
  ROUND(SUM(file_size_bytes) / POW(1024, 2), 2) as total_size_mb,
  ROUND(SUM(file_size_bytes) / POW(1024, 3), 2) as total_size_gb,
  MIN(timestamp) as earliest_timestamp,
  MAX(timestamp) as latest_timestamp,
  MIN(created_time) as earliest_created,
  MAX(created_time) as latest_created,
  ROUND(AVG(file_size_bytes), 2) as avg_file_size_bytes,
  COUNT(DISTINCT feed_url) as unique_feed_urls,
  COUNT(DISTINCT DATE(timestamp)) as days_collected
FROM `{PROJECT_ID}.{DATASET}.all_raw_files_metadata`
GROUP BY environment, feed_type
ORDER BY environment, feed_type;

-- =============================================================================
-- View: Daily collection statistics
-- =============================================================================
CREATE OR REPLACE VIEW `{PROJECT_ID}.{DATASET}.daily_collection_stats` AS
SELECT
  environment,
  feed_type,
  DATE(timestamp) as collection_date,
  COUNT(*) as files_collected,
  SUM(file_size_bytes) as total_bytes,
  ROUND(SUM(file_size_bytes) / POW(1024, 2), 2) as total_mb,
  COUNT(DISTINCT feed_url) as unique_feeds,
  MIN(timestamp) as first_collection,
  MAX(timestamp) as last_collection,
  TIMESTAMP_DIFF(MAX(timestamp), MIN(timestamp), MINUTE) as collection_span_minutes
FROM `{PROJECT_ID}.{DATASET}.all_raw_files_metadata`
GROUP BY environment, feed_type, DATE(timestamp)
ORDER BY environment, feed_type, collection_date;

-- =============================================================================
-- View: Hourly collection patterns
-- =============================================================================
CREATE OR REPLACE VIEW `{PROJECT_ID}.{DATASET}.hourly_collection_patterns` AS
SELECT
  environment,
  feed_type,
  EXTRACT(HOUR FROM timestamp) as hour_of_day,
  COUNT(*) as total_files,
  ROUND(AVG(file_size_bytes), 2) as avg_file_size,
  COUNT(DISTINCT DATE(timestamp)) as days_observed
FROM `{PROJECT_ID}.{DATASET}.all_raw_files_metadata`
GROUP BY environment, feed_type, hour_of_day
ORDER BY environment, feed_type, hour_of_day;

-- =============================================================================
-- View: Feed URL analysis
-- =============================================================================
CREATE OR REPLACE VIEW `{PROJECT_ID}.{DATASET}.feed_url_analysis` AS
SELECT
  environment,
  feed_type,
  feed_url,
  COUNT(*) as total_fetches,
  SUM(file_size_bytes) as total_bytes,
  ROUND(AVG(file_size_bytes), 2) as avg_file_size,
  MIN(timestamp) as first_fetch,
  MAX(timestamp) as last_fetch,
  TIMESTAMP_DIFF(MAX(timestamp), MIN(timestamp), DAY) as days_between_first_last,
  COUNT(DISTINCT DATE(timestamp)) as days_fetched
FROM `{PROJECT_ID}.{DATASET}.all_raw_files_metadata`
GROUP BY environment, feed_type, feed_url
ORDER BY environment, feed_type, total_fetches DESC;

-- =============================================================================
-- View: Data collection gaps (missing hours)
-- =============================================================================
CREATE OR REPLACE VIEW `{PROJECT_ID}.{DATASET}.collection_gaps` AS
WITH
  date_range AS (
    SELECT
      environment,
      feed_type,
      MIN(DATE(timestamp)) as start_date,
      MAX(DATE(timestamp)) as end_date
    FROM `{PROJECT_ID}.{DATASET}.all_raw_files_metadata`
    GROUP BY environment, feed_type
  ),
  expected_hours AS (
    SELECT
      dr.environment,
      dr.feed_type,
      TIMESTAMP_ADD(TIMESTAMP(dr.start_date), INTERVAL hour HOUR) as expected_hour
    FROM date_range dr
    CROSS JOIN UNNEST(GENERATE_ARRAY(0, TIMESTAMP_DIFF(TIMESTAMP(dr.end_date), TIMESTAMP(dr.start_date), HOUR))) as hour
  ),
  actual_hours AS (
    SELECT DISTINCT
      environment,
      feed_type,
      TIMESTAMP_TRUNC(timestamp, HOUR) as actual_hour
    FROM `{PROJECT_ID}.{DATASET}.all_raw_files_metadata`
  )
SELECT
  eh.environment,
  eh.feed_type,
  eh.expected_hour as missing_hour,
  DATE(eh.expected_hour) as missing_date,
  EXTRACT(HOUR FROM eh.expected_hour) as missing_hour_of_day
FROM expected_hours eh
LEFT JOIN actual_hours ah
  ON eh.environment = ah.environment
  AND eh.feed_type = ah.feed_type
  AND eh.expected_hour = ah.actual_hour
WHERE ah.actual_hour IS NULL
ORDER BY eh.environment, eh.feed_type, eh.expected_hour;

-- =============================================================================
-- View: File size trends over time
-- =============================================================================
CREATE OR REPLACE VIEW `{PROJECT_ID}.{DATASET}.file_size_trends` AS
SELECT
  environment,
  feed_type,
  feed_url,
  DATE(timestamp) as collection_date,
  COUNT(*) as files_per_day,
  MIN(file_size_bytes) as min_size,
  MAX(file_size_bytes) as max_size,
  ROUND(AVG(file_size_bytes), 2) as avg_size,
  APPROX_QUANTILES(file_size_bytes, 100)[OFFSET(50)] as median_size,
  STDDEV(file_size_bytes) as stddev_size
FROM `{PROJECT_ID}.{DATASET}.all_raw_files_metadata`
GROUP BY environment, feed_type, feed_url, DATE(timestamp)
ORDER BY environment, feed_type, feed_url, collection_date;

-- =============================================================================
-- View: Comparison between test and prod
-- =============================================================================
CREATE OR REPLACE VIEW `{PROJECT_ID}.{DATASET}.test_vs_prod_comparison` AS
WITH stats AS (
  SELECT
    feed_type,
    environment,
    COUNT(*) as file_count,
    SUM(file_size_bytes) as total_bytes,
    MIN(timestamp) as earliest_file,
    MAX(timestamp) as latest_file,
    COUNT(DISTINCT feed_url) as unique_urls,
    COUNT(DISTINCT DATE(timestamp)) as collection_days
  FROM `{PROJECT_ID}.{DATASET}.all_raw_files_metadata`
  GROUP BY feed_type, environment
)
SELECT
  t.feed_type,
  t.file_count as test_file_count,
  p.file_count as prod_file_count,
  t.file_count - p.file_count as count_diff,
  ROUND(t.total_bytes / POW(1024, 3), 2) as test_size_gb,
  ROUND(p.total_bytes / POW(1024, 3), 2) as prod_size_gb,
  t.unique_urls as test_unique_urls,
  p.unique_urls as prod_unique_urls,
  t.collection_days as test_days,
  p.collection_days as prod_days,
  t.earliest_file as test_earliest,
  p.earliest_file as prod_earliest,
  t.latest_file as test_latest,
  p.latest_file as prod_latest
FROM stats t
FULL OUTER JOIN stats p ON t.feed_type = p.feed_type AND p.environment = 'prod'
WHERE t.environment = 'test'
ORDER BY t.feed_type;
-- =============================================================================
-- PARSED BUCKETS METADATA
-- =============================================================================

-- =============================================================================
-- External Table: Test Bucket Parsed File Metadata
-- =============================================================================
CREATE OR REPLACE EXTERNAL TABLE `{PROJECT_ID}.{DATASET}.test_parsed_files_metadata`
(
  table_name STRING OPTIONS(description="Table name (e.g., gtfs_rt__vehicle_positions, gtfs_schedule__stops_txt)"),
  date DATE OPTIONS(description="Date from dt= partition"),
  hour TIMESTAMP OPTIONS(description="Hour from hour= partition"),
  feed_url STRING OPTIONS(description="Decoded feed URL from base64url filename"),
  file_path STRING OPTIONS(description="Full gs:// path"),
  file_size_bytes INT64 OPTIONS(description="File size in bytes"),
  created_time TIMESTAMP OPTIONS(description="GCS file creation timestamp"),
  file_type STRING OPTIONS(description="File type: data (.jsonl.gz), outcomes (.jsonl), or other")
)
OPTIONS (
  format = 'CSV',
  uris = ['gs://transit-data-demo-metadata/test-jarvus-transit-data-demo-parsed-files.csv'],
  skip_leading_rows = 1,
  description = 'Metadata for all files in test-jarvus-transit-data-demo-parsed bucket'
);

-- =============================================================================
-- External Table: Production Bucket Parsed File Metadata
-- =============================================================================
CREATE OR REPLACE EXTERNAL TABLE `{PROJECT_ID}.{DATASET}.prod_parsed_files_metadata`
(
  table_name STRING OPTIONS(description="Table name (e.g., gtfs_rt__vehicle_positions, gtfs_schedule__stops_txt)"),
  date DATE OPTIONS(description="Date from dt= partition"),
  hour TIMESTAMP OPTIONS(description="Hour from hour= partition"),
  feed_url STRING OPTIONS(description="Decoded feed URL from base64url filename"),
  file_path STRING OPTIONS(description="Full gs:// path"),
  file_size_bytes INT64 OPTIONS(description="File size in bytes"),
  created_time TIMESTAMP OPTIONS(description="GCS file creation timestamp"),
  file_type STRING OPTIONS(description="File type: data (.jsonl.gz), outcomes (.jsonl), or other")
)
OPTIONS (
  format = 'CSV',
  uris = ['gs://transit-data-demo-metadata/jarvus-transit-data-demo-parsed-files.csv'],
  skip_leading_rows = 1,
  description = 'Metadata for all files in jarvus-transit-data-demo-parsed bucket'
);

-- =============================================================================
-- View: Combined parsed metadata from both buckets
-- =============================================================================
CREATE OR REPLACE VIEW `{PROJECT_ID}.{DATASET}.all_parsed_files_metadata` AS
SELECT
  'test' AS environment,
  *
FROM `{PROJECT_ID}.{DATASET}.test_parsed_files_metadata`

UNION ALL

SELECT
  'prod' AS environment,
  *
FROM `{PROJECT_ID}.{DATASET}.prod_parsed_files_metadata`;

-- =============================================================================
-- View: Parsed file collection summary by table
-- =============================================================================
CREATE OR REPLACE VIEW `{PROJECT_ID}.{DATASET}.parsed_file_collection_summary` AS
SELECT
  environment,
  table_name,
  file_type,
  COUNT(*) as total_files,
  SUM(file_size_bytes) as total_size_bytes,
  ROUND(SUM(file_size_bytes) / POW(1024, 2), 2) as total_size_mb,
  ROUND(SUM(file_size_bytes) / POW(1024, 3), 2) as total_size_gb,
  MIN(hour) as earliest_hour,
  MAX(hour) as latest_hour,
  MIN(created_time) as earliest_created,
  MAX(created_time) as latest_created,
  ROUND(AVG(file_size_bytes), 2) as avg_file_size_bytes,
  COUNT(DISTINCT feed_url) as unique_feed_urls,
  COUNT(DISTINCT date) as days_collected,
  COUNT(DISTINCT TIMESTAMP_TRUNC(hour, HOUR)) as hours_collected
FROM `{PROJECT_ID}.{DATASET}.all_parsed_files_metadata`
GROUP BY environment, table_name, file_type
ORDER BY environment, table_name, file_type;

-- =============================================================================
-- View: Parsed data daily statistics
-- =============================================================================
CREATE OR REPLACE VIEW `{PROJECT_ID}.{DATASET}.parsed_daily_stats` AS
SELECT
  environment,
  table_name,
  file_type,
  date as collection_date,
  COUNT(*) as files_collected,
  SUM(file_size_bytes) as total_bytes,
  ROUND(SUM(file_size_bytes) / POW(1024, 2), 2) as total_mb,
  COUNT(DISTINCT feed_url) as unique_feeds,
  MIN(hour) as first_hour,
  MAX(hour) as last_hour,
  COUNT(DISTINCT TIMESTAMP_TRUNC(hour, HOUR)) as hours_with_data
FROM `{PROJECT_ID}.{DATASET}.all_parsed_files_metadata`
GROUP BY environment, table_name, file_type, date
ORDER BY environment, table_name, file_type, collection_date;

-- =============================================================================
-- View: Raw to Parsed comparison (file counts and compression)
-- =============================================================================
CREATE OR REPLACE VIEW `{PROJECT_ID}.{DATASET}.raw_to_parsed_comparison` AS
WITH raw_hourly AS (
  SELECT
    environment,
    feed_type,
    DATE(timestamp) as date,
    TIMESTAMP_TRUNC(timestamp, HOUR) as hour,
    COUNT(*) as raw_files,
    SUM(file_size_bytes) as raw_bytes
  FROM `{PROJECT_ID}.{DATASET}.all_raw_files_metadata`
  GROUP BY environment, feed_type, date, hour
),
parsed_hourly AS (
  SELECT
    environment,
    table_name,
    date,
    TIMESTAMP_TRUNC(hour, HOUR) as hour,
    SUM(CASE WHEN file_type = 'data' THEN 1 ELSE 0 END) as parsed_data_files,
    SUM(CASE WHEN file_type = 'data' THEN file_size_bytes ELSE 0 END) as parsed_data_bytes,
    SUM(CASE WHEN file_type = 'outcomes' THEN 1 ELSE 0 END) as parsed_outcome_files,
    SUM(CASE WHEN file_type = 'outcomes' THEN file_size_bytes ELSE 0 END) as parsed_outcome_bytes
  FROM `{PROJECT_ID}.{DATASET}.all_parsed_files_metadata`
  GROUP BY environment, table_name, date, hour
)
SELECT
  r.environment,
  r.feed_type,
  p.table_name,
  r.date,
  r.hour,
  r.raw_files,
  r.raw_bytes,
  ROUND(r.raw_bytes / POW(1024, 2), 2) as raw_mb,
  p.parsed_data_files,
  p.parsed_data_bytes,
  ROUND(p.parsed_data_bytes / POW(1024, 2), 2) as parsed_data_mb,
  ROUND((r.raw_bytes - p.parsed_data_bytes) / r.raw_bytes * 100, 2) as compression_pct,
  p.parsed_outcome_files,
  p.parsed_outcome_bytes
FROM raw_hourly r
LEFT JOIN parsed_hourly p
  ON r.environment = p.environment
  AND r.feed_type = REGEXP_EXTRACT(p.table_name, r'^(.+?)(__outcomes)?$', 1)
  AND r.date = p.date
  AND r.hour = p.hour
ORDER BY r.environment, r.feed_type, r.date, r.hour;

-- =============================================================================
-- View: Parsed table types analysis
-- =============================================================================
CREATE OR REPLACE VIEW `{PROJECT_ID}.{DATASET}.parsed_table_types` AS
SELECT
  environment,
  CASE
    WHEN table_name LIKE 'gtfs_rt%' THEN 'GTFS Realtime'
    WHEN table_name LIKE 'gtfs_schedule%' THEN 'GTFS Schedule'
    WHEN table_name LIKE 'septa%' THEN 'SEPTA API'
    ELSE 'Other'
  END as data_source,
  CASE
    WHEN table_name LIKE '%__outcomes' THEN REGEXP_EXTRACT(table_name, r'^(.+)__outcomes$', 1)
    ELSE table_name
  END as base_table,
  COUNTIF(table_name LIKE '%__outcomes') as has_outcomes,
  COUNT(DISTINCT table_name) as table_variants,
  SUM(total_files) as total_files,
  SUM(total_size_gb) as total_size_gb
FROM `{PROJECT_ID}.{DATASET}.parsed_file_collection_summary`
GROUP BY environment, data_source, base_table
ORDER BY environment, data_source, total_size_gb DESC;

-- =============================================================================
-- View: Parsed outcomes analysis (parse success rates)
-- =============================================================================
CREATE OR REPLACE VIEW `{PROJECT_ID}.{DATASET}.parsed_outcomes_summary` AS
SELECT
  environment,
  REGEXP_EXTRACT(table_name, r'^(.+)__outcomes$', 1) as base_table,
  date,
  COUNT(*) as outcome_files,
  SUM(file_size_bytes) as outcome_bytes,
  MIN(hour) as earliest_hour,
  MAX(hour) as latest_hour
FROM `{PROJECT_ID}.{DATASET}.all_parsed_files_metadata`
WHERE file_type = 'outcomes'
GROUP BY environment, base_table, date
ORDER BY environment, base_table, date;

-- =============================================================================
-- View: Test vs Prod parsed data comparison
-- =============================================================================
CREATE OR REPLACE VIEW `{PROJECT_ID}.{DATASET}.test_vs_prod_parsed_comparison` AS
WITH stats AS (
  SELECT
    table_name,
    file_type,
    environment,
    COUNT(*) as file_count,
    SUM(file_size_bytes) as total_bytes,
    MIN(hour) as earliest_hour,
    MAX(hour) as latest_hour,
    COUNT(DISTINCT feed_url) as unique_urls,
    COUNT(DISTINCT date) as collection_days
  FROM `{PROJECT_ID}.{DATASET}.all_parsed_files_metadata`
  GROUP BY table_name, file_type, environment
)
SELECT
  t.table_name,
  t.file_type,
  t.file_count as test_file_count,
  p.file_count as prod_file_count,
  t.file_count - IFNULL(p.file_count, 0) as count_diff,
  ROUND(t.total_bytes / POW(1024, 3), 2) as test_size_gb,
  ROUND(IFNULL(p.total_bytes, 0) / POW(1024, 3), 2) as prod_size_gb,
  t.unique_urls as test_unique_urls,
  IFNULL(p.unique_urls, 0) as prod_unique_urls,
  t.collection_days as test_days,
  IFNULL(p.collection_days, 0) as prod_days,
  t.earliest_hour as test_earliest,
  p.earliest_hour as prod_earliest,
  t.latest_hour as test_latest,
  p.latest_hour as prod_latest
FROM stats t
FULL OUTER JOIN stats p
  ON t.table_name = p.table_name
  AND t.file_type = p.file_type
  AND p.environment = 'prod'
WHERE t.environment = 'test'
ORDER BY t.table_name, t.file_type;

-- =============================================================================
-- BUCKET COVERAGE SUMMARY TABLES
-- =============================================================================

-- =============================================================================
-- External Table: Test Raw Bucket Coverage Summary
-- =============================================================================
CREATE OR REPLACE EXTERNAL TABLE `{PROJECT_ID}.{DATASET}.test_raw_bucket_coverage`
(
  environment STRING OPTIONS(description="Environment (test/prod)"),
  bucket STRING OPTIONS(description="GCS bucket name"),
  bucket_type STRING OPTIONS(description="Bucket type (raw/parsed)"),
  feed_type STRING OPTIONS(description="Feed type (e.g., gtfs_rt__vehicle_positions)"),
  feed_url STRING OPTIONS(description="Feed URL"),
  first_date DATE OPTIONS(description="First date with data"),
  last_date DATE OPTIONS(description="Last date with data"),
  first_timestamp TIMESTAMP OPTIONS(description="First timestamp"),
  last_timestamp TIMESTAMP OPTIONS(description="Last timestamp"),
  total_files INT64 OPTIONS(description="Total number of files"),
  total_size_bytes INT64 OPTIONS(description="Total size in bytes"),
  total_size_gb FLOAT64 OPTIONS(description="Total size in GB"),
  days_covered INT64 OPTIONS(description="Number of days with data"),
  hours_covered INT64 OPTIONS(description="Number of hours with data"),
  has_gaps BOOL OPTIONS(description="Whether there are gaps in hourly coverage"),
  gap_count INT64 OPTIONS(description="Number of missing hours")
)
OPTIONS (
  format = 'CSV',
  uris = ['gs://transit-data-demo-metadata/test-jarvus-transit-data-demo-raw-coverage.csv'],
  skip_leading_rows = 1,
  description = 'Coverage summary for test-jarvus-transit-data-demo-raw bucket'
);

-- =============================================================================
-- External Table: Production Raw Bucket Coverage Summary
-- =============================================================================
CREATE OR REPLACE EXTERNAL TABLE `{PROJECT_ID}.{DATASET}.prod_raw_bucket_coverage`
(
  environment STRING OPTIONS(description="Environment (test/prod)"),
  bucket STRING OPTIONS(description="GCS bucket name"),
  bucket_type STRING OPTIONS(description="Bucket type (raw/parsed)"),
  feed_type STRING OPTIONS(description="Feed type (e.g., gtfs_rt__vehicle_positions)"),
  feed_url STRING OPTIONS(description="Feed URL"),
  first_date DATE OPTIONS(description="First date with data"),
  last_date DATE OPTIONS(description="Last date with data"),
  first_timestamp TIMESTAMP OPTIONS(description="First timestamp"),
  last_timestamp TIMESTAMP OPTIONS(description="Last timestamp"),
  total_files INT64 OPTIONS(description="Total number of files"),
  total_size_bytes INT64 OPTIONS(description="Total size in bytes"),
  total_size_gb FLOAT64 OPTIONS(description="Total size in GB"),
  days_covered INT64 OPTIONS(description="Number of days with data"),
  hours_covered INT64 OPTIONS(description="Number of hours with data"),
  has_gaps BOOL OPTIONS(description="Whether there are gaps in hourly coverage"),
  gap_count INT64 OPTIONS(description="Number of missing hours")
)
OPTIONS (
  format = 'CSV',
  uris = ['gs://transit-data-demo-metadata/jarvus-transit-data-demo-raw-coverage.csv'],
  skip_leading_rows = 1,
  description = 'Coverage summary for jarvus-transit-data-demo-raw bucket'
);

-- =============================================================================
-- External Table: Test Parsed Bucket Coverage Summary
-- =============================================================================
CREATE OR REPLACE EXTERNAL TABLE `{PROJECT_ID}.{DATASET}.test_parsed_bucket_coverage`
(
  environment STRING OPTIONS(description="Environment (test/prod)"),
  bucket STRING OPTIONS(description="GCS bucket name"),
  bucket_type STRING OPTIONS(description="Bucket type (raw/parsed)"),
  feed_type STRING OPTIONS(description="Table name"),
  feed_url STRING OPTIONS(description="Feed URL"),
  first_date DATE OPTIONS(description="First date with data"),
  last_date DATE OPTIONS(description="Last date with data"),
  first_timestamp TIMESTAMP OPTIONS(description="First timestamp"),
  last_timestamp TIMESTAMP OPTIONS(description="Last timestamp"),
  total_files INT64 OPTIONS(description="Total number of files"),
  total_size_bytes INT64 OPTIONS(description="Total size in bytes"),
  total_size_gb FLOAT64 OPTIONS(description="Total size in GB"),
  days_covered INT64 OPTIONS(description="Number of days with data"),
  hours_covered INT64 OPTIONS(description="Number of hours with data"),
  has_gaps BOOL OPTIONS(description="Whether there are gaps in hourly coverage"),
  gap_count INT64 OPTIONS(description="Number of missing hours")
)
OPTIONS (
  format = 'CSV',
  uris = ['gs://transit-data-demo-metadata/test-jarvus-transit-data-demo-parsed-coverage.csv'],
  skip_leading_rows = 1,
  description = 'Coverage summary for test-jarvus-transit-data-demo-parsed bucket'
);

-- =============================================================================
-- External Table: Production Parsed Bucket Coverage Summary
-- =============================================================================
CREATE OR REPLACE EXTERNAL TABLE `{PROJECT_ID}.{DATASET}.prod_parsed_bucket_coverage`
(
  environment STRING OPTIONS(description="Environment (test/prod)"),
  bucket STRING OPTIONS(description="GCS bucket name"),
  bucket_type STRING OPTIONS(description="Bucket type (raw/parsed)"),
  feed_type STRING OPTIONS(description="Table name"),
  feed_url STRING OPTIONS(description="Feed URL"),
  first_date DATE OPTIONS(description="First date with data"),
  last_date DATE OPTIONS(description="Last date with data"),
  first_timestamp TIMESTAMP OPTIONS(description="First timestamp"),
  last_timestamp TIMESTAMP OPTIONS(description="Last timestamp"),
  total_files INT64 OPTIONS(description="Total number of files"),
  total_size_bytes INT64 OPTIONS(description="Total size in bytes"),
  total_size_gb FLOAT64 OPTIONS(description="Total size in GB"),
  days_covered INT64 OPTIONS(description="Number of days with data"),
  hours_covered INT64 OPTIONS(description="Number of hours with data"),
  has_gaps BOOL OPTIONS(description="Whether there are gaps in hourly coverage"),
  gap_count INT64 OPTIONS(description="Number of missing hours")
)
OPTIONS (
  format = 'CSV',
  uris = ['gs://transit-data-demo-metadata/jarvus-transit-data-demo-parsed-coverage.csv'],
  skip_leading_rows = 1,
  description = 'Coverage summary for jarvus-transit-data-demo-parsed bucket'
);

-- =============================================================================
-- View: All Bucket Coverage (raw and parsed, test and prod)
-- =============================================================================
CREATE OR REPLACE VIEW `{PROJECT_ID}.{DATASET}.all_bucket_coverage` AS
SELECT * FROM `{PROJECT_ID}.{DATASET}.test_raw_bucket_coverage`
UNION ALL
SELECT * FROM `{PROJECT_ID}.{DATASET}.prod_raw_bucket_coverage`
UNION ALL
SELECT * FROM `{PROJECT_ID}.{DATASET}.test_parsed_bucket_coverage`
UNION ALL
SELECT * FROM `{PROJECT_ID}.{DATASET}.prod_parsed_bucket_coverage`;

-- =============================================================================
-- View: Coverage Summary by Feed Type
-- =============================================================================
CREATE OR REPLACE VIEW `{PROJECT_ID}.{DATASET}.coverage_by_feed_type` AS
SELECT
  environment,
  bucket_type,
  feed_type,
  COUNT(DISTINCT feed_url) as unique_feed_urls,
  SUM(total_files) as total_files,
  SUM(total_size_gb) as total_size_gb,
  MIN(first_date) as earliest_date,
  MAX(last_date) as latest_date,
  SUM(days_covered) as total_days_covered,
  SUM(hours_covered) as total_hours_covered,
  COUNTIF(has_gaps) as feeds_with_gaps,
  SUM(gap_count) as total_gaps
FROM `{PROJECT_ID}.{DATASET}.all_bucket_coverage`
GROUP BY environment, bucket_type, feed_type
ORDER BY environment, bucket_type, feed_type;

-- =============================================================================
-- View: Data Completeness Analysis
-- =============================================================================
CREATE OR REPLACE VIEW `{PROJECT_ID}.{DATASET}.data_completeness` AS
WITH expected_hours AS (
  SELECT
    environment,
    bucket_type,
    feed_type,
    feed_url,
    days_covered,
    hours_covered,
    gap_count,
    -- Calculate expected hours (days * 24)
    days_covered * 24 as max_possible_hours,
    -- Actual hours + gaps should equal expected
    hours_covered + gap_count as calculated_expected_hours
  FROM `{PROJECT_ID}.{DATASET}.all_bucket_coverage`
)
SELECT
  *,
  ROUND(hours_covered / NULLIF(calculated_expected_hours, 0) * 100, 2) as completeness_pct,
  CASE
    WHEN gap_count = 0 THEN 'Complete'
    WHEN gap_count < hours_covered * 0.1 THEN 'Mostly Complete (< 10% gaps)'
    WHEN gap_count < hours_covered * 0.5 THEN 'Partial (10-50% gaps)'
    ELSE 'Sparse (> 50% gaps)'
  END as completeness_category
FROM expected_hours
ORDER BY environment, bucket_type, feed_type, feed_url;

-- =============================================================================
-- View: Largest Feeds by Storage
-- =============================================================================
CREATE OR REPLACE VIEW `{PROJECT_ID}.{DATASET}.largest_feeds` AS
SELECT
  environment,
  bucket_type,
  feed_type,
  feed_url,
  total_files,
  total_size_gb,
  ROUND(total_size_gb / days_covered, 4) as gb_per_day,
  ROUND(total_files / days_covered, 2) as files_per_day,
  days_covered,
  hours_covered,
  has_gaps,
  gap_count
FROM `{PROJECT_ID}.{DATASET}.all_bucket_coverage`
ORDER BY total_size_gb DESC
LIMIT 50;
