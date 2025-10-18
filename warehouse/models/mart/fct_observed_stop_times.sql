with shape_times as (
    select * from {{ ref('fct_observed_shape_times') }}
    -- only keep rows where we have a progress %
    -- https://github.com/JarvusInnovations/transit-data-analytics-demo/issues/36
    where shape_closest_point_to_vehicle_position_as_pct is not null
),

{# schedule as (
    select * from {{ ref('fct_scheduled_stops') }}
), #}

-- todo:
-- 1. lag points within the observed journey to get progress along shape
-- 2. join scheduled stops into the observed segment that contained it
-- 3. interpolate stop time based on time between observed pings
-- there is probably a better way to do this but this should work for back of envelope

fct_observed_stop_times as (
    select
        shape_times._b64_url,
        shape_times.schedule_b64_url,
        shape_times.dt,
        shape_times.service_date,
        shape_times.vehicle_timestamp,
        shape_times.latitude,
        shape_times.longitude,
        shape_times.current_stop_sequence,
        shape_times.trip_id,
        shape_times.vehicle_id,
        shape_times.trip_schedule_relationship,
        shape_times.shape_id,
        shape_times.trip_route_id,
        shape_times.shape_closest_point_to_vehicle_position,
        shape_times.shape_closest_point_to_vehicle_position_as_pct
    from shape_times
)

select * from fct_observed_stop_times
