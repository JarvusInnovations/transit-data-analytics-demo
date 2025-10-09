{{ config(materialized='table') }}

with fct_vehicle_positions as (
    select distinct --noqa: ST06
        _b64_url,
        dt,
        extract(date from vehicle_timestamp) as service_date,
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
    where dt in ('2024-02-08', '2024-02-07', '2024-02-09')
)

select * from fct_vehicle_positions
