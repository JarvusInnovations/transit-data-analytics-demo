{{ config(materialized='table') }}

with fct_vehicle_positions as (
    select distinct
        _b64_url,
        dt,
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
    where dt = '2024-02-19'
)

select * from fct_vehicle_positions
