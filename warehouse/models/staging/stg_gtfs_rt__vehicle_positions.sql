WITH src AS (
    SELECT * FROM {{ source('transit_data', 'gtfs_rt__vehicle_positions') }}
),

unpack_json AS (
    SELECT
        {{ read_file_config_and_partitions() }},
        {{ read_gtfs_rt_trip_descriptor('entity.vehicle') }}
    FROM src

),

stg_gtfs_rt__vehicle_positions AS (
    SELECT
        {{ metadata_columns() }},
        {{ trip_descriptor_columns() }}
    FROM unpack_json
)

SELECT * FROM stg_gtfs_rt__vehicle_positions
