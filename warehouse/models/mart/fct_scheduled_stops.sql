with stop_times as (
    select
        *,
        {{ gtfs_time_to_seconds('arrival_time') }} as arrival_secs,
        {{ gtfs_time_to_seconds('departure_time') }} as departure_secs
    from {{ ref('stg_stop_times') }}
),

stops as (
    select * from {{ ref('stg_stops') }}
),

trips as (
    select * from {{ ref('fct_scheduled_trips') }}
),

fct_scheduled_stops as (
    select
        stop_times._b64_url,
        trips.feed_name,
        stop_times.dt,
        stop_times.stop_id,
        stop_times.stop_sequence,
        trips.service_date,
        trips.service_id,
        trips.route_id,
        trips.route_type,
        stops.stop_lat,
        stops.stop_lon,
        MAKE_INTERVAL(second => stop_times.arrival_secs) + trips.service_date as arrival_time,
        MAKE_INTERVAL(second => stop_times.departure_secs) + trips.service_date as departure_time
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
)

select * from fct_scheduled_stops
