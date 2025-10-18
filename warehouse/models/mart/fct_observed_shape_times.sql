{{ config(materialized='table') }}

with vehicle_positions as (
    select
        *,
        st_geogpoint(longitude, latitude) as location_geog
    from {{ ref('fct_vehicle_positions') }}
    -- ignore data with no trip, this could be imputed but deferring that
    where trip_id is not null
),

feed_map as (
    select * from {{ ref('feed_map') }}
),

trips as (
    select * from {{ ref('fct_scheduled_trips') }}
),

shapes as (
    select * from {{ ref('dim_shapes') }}
),

fct_observed_shape_times as (
    select --noqa: ST06
        vehicle_positions._b64_url,
        feed_map.schedule_b64_url,
        vehicle_positions.dt,
        vehicle_positions.service_date,
        vehicle_positions.vehicle_timestamp,
        vehicle_positions.latitude,
        vehicle_positions.longitude,
        vehicle_positions.current_stop_sequence,
        vehicle_positions.trip_id,
        vehicle_positions.vehicle_id,
        vehicle_positions.trip_schedule_relationship,
        shapes.shape_id,
        trips.route_id as trip_route_id,
        st_closestpoint(shapes.shape_linestring, vehicle_positions.location_geog) as shape_closest_point_to_vehicle_position,
        case
            when not st_iscollection(shapes.shape_linestring) then st_linelocatepoint(shapes.shape_linestring, st_closestpoint(shapes.shape_linestring, vehicle_positions.location_geog))
        end as shape_closest_point_to_vehicle_position_as_pct
    from vehicle_positions
    left join feed_map
        on vehicle_positions._b64_url = feed_map.rt_b64_url
    left join trips
        on
            feed_map.schedule_b64_url = trips._b64_url
            and vehicle_positions.service_date = trips.service_date
            and vehicle_positions.trip_id = trips.trip_id
    left join shapes
        on
            trips._b64_url = shapes._b64_url
            and trips.dt = shapes._valid_from
            and trips.shape_id = shapes.shape_id
)

select * from fct_observed_shape_times
