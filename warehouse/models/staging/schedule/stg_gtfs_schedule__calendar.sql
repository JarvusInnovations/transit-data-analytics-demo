WITH src AS (
    SELECT * FROM {{ source('transit_data', 'gtfs_schedule__calendar') }}
),

unpack_json AS (
    SELECT
        {{ read_file_config_and_partitions() }},
        JSON_VALUE(record, '$.service_id') AS service_id,
        JSON_VALUE(record, '$.monday') AS monday,
        JSON_VALUE(record, '$.tuesday') AS tuesday,
        JSON_VALUE(record, '$.wednesday') AS wednesday,
        JSON_VALUE(record, '$.thursday') AS thursday,
        JSON_VALUE(record, '$.friday') AS friday,
        JSON_VALUE(record, '$.saturday') AS saturday,
        JSON_VALUE(record, '$.sunday') AS sunday,
        JSON_VALUE(record, '$.start_date') AS start_date,
        JSON_VALUE(record, '$.end_date') AS end_date,
    FROM src
),

stg_gtfs_schedule__calendar AS (
    SELECT
        dt,
        hour,
        _url,
        _name,
        _extract_ts,
        service_id,
        CAST(CAST(monday AS INTEGER) AS BOOLEAN) AS monday,
        CAST(CAST(tuesday AS INTEGER) AS BOOLEAN) AS tuesday,
        CAST(CAST(wednesday AS INTEGER) AS BOOLEAN) AS wednesday,
        CAST(CAST(thursday AS INTEGER) AS BOOLEAN) AS thursday,
        CAST(CAST(friday AS INTEGER) AS BOOLEAN) AS friday,
        CAST(CAST(saturday AS INTEGER) AS BOOLEAN) AS saturday,
        CAST(CAST(sunday AS INTEGER) AS BOOLEAN) AS sunday,
        PARSE_DATE("%Y%m%d", start_date) AS start_date,
        PARSE_DATE("%Y%m%d", end_date) AS end_date,
    FROM unpack_json
)

SELECT * FROM stg_gtfs_schedule__calendar
