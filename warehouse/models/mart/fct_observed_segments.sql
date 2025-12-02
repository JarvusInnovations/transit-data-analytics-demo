{{ config(materialized='table') }}

with stop_times as (
    select * from {{ ref('fct_observed_stop_times') }}
),

make_segments as (
    select distinct
        feed_key,
        stop_key,
        feed_name,
        route_id,
        schedule_b64_url,
        schedule_dt,
        stop_id,
        stop_name,
        shape_id,
        shape_closest_point_to_stop_as_pct,
        lead(shape_closest_point_to_stop_as_pct) over (partition by schedule_b64_url, schedule_dt, trip_id, service_date order by scheduled_stop_sequence) as next_stop_pct,
        lead(stop_id) over (partition by schedule_b64_url, schedule_dt, trip_id, service_date order by scheduled_stop_sequence) as next_stop_id,
        lead(stop_name) over (partition by schedule_b64_url, schedule_dt, trip_id, service_date order by scheduled_stop_sequence) as next_stop_name,
        lead(stop_key) over (partition by schedule_b64_url, schedule_dt, trip_id, service_date order by scheduled_stop_sequence) as next_stop_key
    from stop_times
),

fct_observed_segments as (
    select --noqa: ST06
        -- todo: find a better way to aggregate when the same stop pair appears in multiple shapes
        -- currently this results in duplicates
        {{ dbt_utils.generate_surrogate_key(['feed_key', 'stop_key', 'next_stop_key', 'shape_id']) }} as segment_key,
        feed_key,
        feed_name,
        route_id,
        stop_id || "-" || next_stop_id as segment_id,
        stop_name || "→" || next_stop_name as segment_name,
        schedule_b64_url,
        schedule_dt,
        stop_id,
        stop_key,
        shape_id,
        stop_name,
        shape_closest_point_to_stop_as_pct,
        next_stop_pct,
        next_stop_id,
        next_stop_name,
        next_stop_key
    from make_segments
    where
        next_stop_id is not null
        and stop_id != next_stop_id
)

select * from fct_observed_segments
