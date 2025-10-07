WITH src AS (
    SELECT
        *,
        _file_name
    FROM {{ source('transit_data', 'gtfs_rt__vehicle_positions') }}
),

unpack_json AS (
    SELECT
        {{ read_gtfs_rt_header() }},
        {{ impute_parsed_rt_file_info() }},
        {{ read_gtfs_rt_trip_descriptor('entity.vehicle') }},
        JSON_VALUE(record, '$.entity.vehicle.position.latitude') AS latitude,
        JSON_VALUE(record, '$.entity.vehicle.position.longitude') AS longitude,
        JSON_VALUE(record, '$.entity.vehicle.position.bearing') AS bearing,
        JSON_VALUE(record, '$.entity.vehicle.position.odometer') AS odometer,
        JSON_VALUE(record, '$.entity.vehicle.position.speed') AS speed,
        JSON_VALUE(record, '$.entity.stopId') AS stop_id,
        JSON_VALUE(record, '$.entity.timestamp') AS vehicle_timestamp,
        JSON_VALUE(record, '$.entity.current_stop_sequence') AS current_stop_sequence,
        JSON_VALUE(record, '$.entity.current_status') AS current_status
    FROM src

),

stg_gtfs_rt__vehicle_positions AS (
    SELECT
        dt,
        hour,
        _b64_url,
        {{ gtfs_rt_header_columns() }},
        {{ trip_descriptor_columns() }},
        latitude,
        longitude,
        bearing,
        odometer,
        speed,
        stop_id,
        vehicle_timestamp,
        current_stop_sequence,
        current_status
    FROM unpack_json
)

SELECT * FROM stg_gtfs_rt__vehicle_positions
