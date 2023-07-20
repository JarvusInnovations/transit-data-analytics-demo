WITH src AS (
    SELECT * FROM {{ source('transit_data', 'gtfs_rt__vehicle_positions') }}
),

stg_gtfs_rt__vehicle_positions AS (
    SELECT
        dt,
        hour,
        TIMESTAMP(STRING(JSON_QUERY(file, '$.ts'))) AS _extract_ts,
        STRING(JSON_QUERY(record, '$.entity.vehicle.trip.tripId')) AS trip_id,
    FROM src
)

SELECT * FROM stg_gtfs_rt__vehicle_positions
