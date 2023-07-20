WITH src AS (
    SELECT * FROM {{ source('transit_data', 'gtfs_rt__trip_updates') }}
),

stg_gtfs_rt__trip_updates AS (
    SELECT
        dt,
        hour,
        TIMESTAMP({{ get_string_value_at_json_path('file', 'ts') }}) AS _extract_ts,
        {{ get_string_value_at_json_path('record', 'entity.tripUpdate.trip.tripId') }} AS trip_id,
        {{ get_string_value_at_json_path('record', 'entity.tripUpdate.trip.routeId') }} AS trip_route_id,
        {{ get_string_value_at_json_path('record', 'entity.tripUpdate.trip.startTime') }} AS trip_start_time,
        {{ get_string_value_at_json_path('record', 'entity.tripUpdate.trip.startDate') }} AS trip_start_date,
        {{ get_string_value_at_json_path('record', 'entity.tripUpdate.trip.scheduleRelationship') }} AS trip_schedule_relationship,
    FROM src
)

SELECT * FROM stg_gtfs_rt__trip_updates
