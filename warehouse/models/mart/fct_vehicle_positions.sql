{{ config(materialized='table') }}

with fct_vehicle_positions as (
    select distinct --noqa: ST06
        _b64_url,
        dt,
        -- todo: fix this based on latest service for a given day per feed?
        -- currently it's messing up some early morning service
        date(timestamp_add(vehicle_timestamp, interval -3 hour), 'America/New_York') as service_date,
        vehicle_timestamp,
        latitude,
        longitude,
        current_stop_sequence,
        trip_id,
        stop_id,
        current_status,
        trip_route_id,
        vehicle_id,
        trip_schedule_relationship
    from {{ ref('stg_gtfs_rt__vehicle_positions') }}
    -- align with schedule
    where
        dt in
        (
            '2023-10-09', '2023-10-10', '2023-10-11', '2023-10-12', '2023-10-13', '2023-10-14', '2023-10-15',
            '2024-10-07', '2024-10-08', '2024-10-09', '2024-10-10', '2024-10-11', '2024-10-12', '2024-10-13'
        )
)

select * from fct_vehicle_positions
