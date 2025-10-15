{{ config(materialized='table') }}

with stop_times as (
    select
        *,
        {{ gtfs_time_to_seconds('arrival_time') }} as arrival_secs,
        {{ gtfs_time_to_seconds('departure_time') }} as departure_secs
    from {{ ref('stg_stop_times') }}
),

stops as (
    select
        *,
        st_geogpoint(stop_lon, stop_lat) as stop_pt
    from {{ ref('stg_stops') }}
),

trips as (
    select * from {{ ref('fct_scheduled_trips') }}
    -- todo: remove this filter
    where dt = '2024-02-08'
),

shapes as (
    select * from {{ ref('dim_shapes') }}
),

fct_scheduled_stops as (
    select --noqa: ST06
        stop_times._b64_url,
        trips.feed_name,
        stop_times.dt,
        stop_times.stop_id,
        stop_times.stop_sequence,
        trips.service_date,
        trips.service_id,
        trips.route_id,
        trips.route_type,
        stops.stop_pt,
        st_closestpoint(shapes.shape_linestring, stops.stop_pt) as shape_closest_point_to_stop,
        case
            when not st_iscollection(shapes.shape_linestring) then st_linelocatepoint(shapes.shape_linestring, st_closestpoint(shapes.shape_linestring, stops.stop_pt))
        end as shape_closest_point_to_stop_as_pct,
        make_interval(second => stop_times.arrival_secs) + trips.service_date as arrival_time,
        make_interval(second => stop_times.departure_secs) + trips.service_date as departure_time,
        stop_times.timepoint
    from trips
    left join stop_times
        on
            trips._b64_url = stop_times._b64_url
            and trips.dt = stop_times.dt
            and trips.trip_id = stop_times.trip_id
    left join stops
        on
            trips._b64_url = stops._b64_url
            and trips.dt = stops.dt
            and stop_times.stop_id = stops.stop_id
    left join shapes
        on
            trips._b64_url = shapes._b64_url
            and trips.dt = shapes._valid_from
            and trips.shape_id = shapes.shape_id
)

select * from fct_scheduled_stops
