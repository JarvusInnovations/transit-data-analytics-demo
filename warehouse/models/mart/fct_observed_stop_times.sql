{{ config(materialized='table') }}

with shape_times as (
    select * from {{ ref('fct_observed_shape_times') }}
    -- only keep rows where we have a progress %
    -- https://github.com/JarvusInnovations/transit-data-analytics-demo/issues/36
    where shape_closest_point_to_vehicle_position_as_pct is not null
),

schedule as (
    select * from {{ ref('fct_scheduled_stops') }}
),

lagged_shape_times as (
    select
        *,
        lag(shape_closest_point_to_vehicle_position_as_pct)
            over (partition by service_date, trip_id order by vehicle_timestamp)
            as previous_ping_shape_pct,
        lag(vehicle_timestamp)
            over (partition by service_date, trip_id order by vehicle_timestamp)
            as previous_ping_timestamp
    from shape_times
),

-- todo:
-- interpolate stop time based on time between observed pings
-- there is probably a better way to do this but this should work for back of envelope

calculated_observed_stop_arrival as (
    select
        lagged_shape_times._b64_url,
        schedule._b64_url as schedule_b64_url,
        lagged_shape_times.dt,
        schedule.service_date,
        lagged_shape_times.vehicle_timestamp,
        lagged_shape_times.latitude,
        lagged_shape_times.longitude,
        lagged_shape_times.current_stop_sequence,
        schedule.trip_id,
        lagged_shape_times.vehicle_id,
        lagged_shape_times.trip_schedule_relationship,
        lagged_shape_times.shape_id,
        schedule.route_id,
        lagged_shape_times.trip_route_id,
        lagged_shape_times.shape_closest_point_to_vehicle_position,
        lagged_shape_times.shape_closest_point_to_vehicle_position_as_pct,
        lagged_shape_times.previous_ping_shape_pct,
        lagged_shape_times.previous_ping_timestamp,
        schedule.stop_id,
        schedule.shape_closest_point_to_stop,
        schedule.shape_closest_point_to_stop_as_pct,
        schedule.stop_pt,
        schedule.arrival_time,
        schedule.departure_time,
        datetime_add(
            lagged_shape_times.previous_ping_timestamp, interval cast(
                (schedule.shape_closest_point_to_stop_as_pct - lagged_shape_times.previous_ping_shape_pct)
                / (lagged_shape_times.shape_closest_point_to_vehicle_position_as_pct - lagged_shape_times.previous_ping_shape_pct)
                * datetime_diff(lagged_shape_times.vehicle_timestamp, lagged_shape_times.previous_ping_timestamp, second)
                as int64
            )
            second
        ) as observed_stop_arrival

    from schedule
    left join lagged_shape_times
        on
            schedule._b64_url = lagged_shape_times.schedule_b64_url
            and schedule.service_date = lagged_shape_times.service_date
            and schedule.trip_id = lagged_shape_times.trip_id
            and schedule.shape_closest_point_to_stop_as_pct
            between lagged_shape_times.previous_ping_shape_pct
            and lagged_shape_times.shape_closest_point_to_vehicle_position_as_pct
    -- todo: improve matching, but for now just drop where no match
    where lagged_shape_times._b64_url is not null
    -- todo: fix this -- if the vehicle stays in one position for a long time, we need to actually handle it
    -- need to take the row where the vehicle leaves the position
    and lagged_shape_times.shape_closest_point_to_vehicle_position_as_pct != lagged_shape_times.previous_ping_shape_pct
),

fct_observed_stop_times as (
    select
        _b64_url as rt_b64_url,
        schedule_b64_url,
        dt as rt_dt,
        service_date,
        vehicle_timestamp,
        latitude,
        longitude,
        current_stop_sequence,
        trip_id,
        vehicle_id,
        trip_schedule_relationship,
        shape_id,
        route_id,
        trip_route_id,
        shape_closest_point_to_vehicle_position,
        shape_closest_point_to_vehicle_position_as_pct,
        previous_ping_shape_pct,
        previous_ping_timestamp,
        stop_id,
        shape_closest_point_to_stop,
        shape_closest_point_to_stop_as_pct,
        stop_pt,
        arrival_time as scheduled_arrival_time,
        departure_time as scheduled_departure_time,
        observed_stop_arrival,
        datetime_diff(observed_stop_arrival, timestamp(arrival_time, 'America/New_York'), second) as diff_from_schedule,
        case
            when datetime_diff(observed_stop_arrival, timestamp(arrival_time, 'America/New_York'), second) < -59 then 'early'
            when datetime_diff(observed_stop_arrival, timestamp(arrival_time, 'America/New_York'), second) > 300 then 'late'
            else 'on_time'
        end as stop_performance_type
    from calculated_observed_stop_arrival
)

select * from fct_observed_stop_times
