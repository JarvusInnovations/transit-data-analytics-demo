WITH src AS (
    SELECT
        *,
        _file_name
    FROM {{ source('transit_data', 'gtfs_rt__trip_updates') }}
),

unpack_json AS (
    SELECT
        {{ read_gtfs_rt_header() }},
        {{ impute_parsed_rt_file_info() }},
        {{ read_gtfs_rt_trip_descriptor('entity.tripUpdate') }},
        {{ read_gtfs_rt_vehicle_descriptor('entity.tripUpdate') }}
    FROM src
),

stg_gtfs_rt__trip_updates AS (
    SELECT
        dt,
        hour,
        _b64_url,
        {{ gtfs_rt_header_columns() }},
        {{ trip_descriptor_columns() }},
        {{ vehicle_descriptor_columns() }}
    FROM unpack_json
)

SELECT * FROM stg_gtfs_rt__trip_updates
