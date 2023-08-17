WITH src AS (
    SELECT * FROM {{ source('transit_data', 'gtfs_rt__trip_updates') }}
),

unpack_json AS (
    SELECT
        {{ read_file_config_and_partitions() }},
        {{ read_gtfs_rt_trip_descriptor('entity.tripUpdate') }}
    FROM src
),

stg_gtfs_rt__trip_updates AS (
    SELECT
        dt,
        hour,
        _url,
        _name,
        _extract_ts,
        {{ trip_descriptor_columns() }}
    FROM unpack_json
)

SELECT * FROM stg_gtfs_rt__trip_updates
