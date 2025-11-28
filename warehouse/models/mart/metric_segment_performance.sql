{{ config(materialized='table') }}

with stop_shape as (
    select * from {{ ref('metric_stop_shape_performance') }}
),

segments as (
    select * from {{ ref('fct_observed_segments') }}
),

segment_geos as (
    select * from {{ ref('fct_observed_segment_geographies') }}
),

routes as (
    select * from {{ ref('stg_routes') }}
),

avg_segments as (
    select
        segments.*,
        first_stop.pick_label,
        first_stop.hour_type,
        first_stop.ct_observations as first_stop_obs,
        second_stop.ct_observations as second_stop_obs,
        first_stop.pct_early as first_stop_pct_early,
        second_stop.pct_early as second_stop_pct_early,
        first_stop.pct_late as first_stop_pct_late,
        second_stop.pct_late as second_stop_pct_late,
        first_stop.pct_on_time as first_stop_pct_on_time,
        second_stop.pct_on_time as second_stop_pct_on_time,
        first_stop.avg_difference_from_schedule_sec as first_stop_avg_difference_from_schedule_sec,
        second_stop.avg_difference_from_schedule_sec as second_stop_avg_difference_from_schedule_sec
    from segments
    left join stop_shape as first_stop
        on
            segments.stop_key = first_stop.stop_key
            and segments.shape_id = first_stop.shape_id
    left join stop_shape as second_stop
        on
            segments.next_stop_key = second_stop.stop_key
            and segments.shape_id = second_stop.shape_id
            and first_stop.pick_label = second_stop.pick_label
            and first_stop.hour_type = second_stop.hour_type

),

metric_segment_performance as (
    select
        avg_segments.feed_key,
        avg_segments.feed_name,
        avg_segments.route_id,
        routes.route_short_name,
        segment_geos.segment_linestring,
        avg_segments.segment_key,
        avg_segments.segment_name,
        avg_segments.hour_type,
        avg_segments.pick_label,
        avg_segments.stop_id,
        avg_segments.schedule_dt,
        avg_segments.schedule_b64_url,
        avg_segments.next_stop_id,
        avg_segments.shape_id,
        -- use minimum of the two stop observation counts as a conservative estimate
        least(avg_segments.first_stop_obs, avg_segments.second_stop_obs) as ct_observations,
        -- should these be weighted averages based on obs?
        -- done this way we assume these values are populated for every stop on the same shape
        -- when there could be trip patterns where they are/are not
        -- we don't have data here for whether the stop was actually serviced vs. just passed
        round(avg_segments.first_stop_pct_early + avg_segments.second_stop_pct_early / 2, 2) as pct_early,
        round(avg_segments.first_stop_pct_late + avg_segments.second_stop_pct_late / 2, 2) as pct_late,
        round(avg_segments.first_stop_pct_on_time + avg_segments.second_stop_pct_on_time / 2, 2) as pct_on_time,
        round(avg_segments.first_stop_avg_difference_from_schedule_sec + avg_segments.second_stop_avg_difference_from_schedule_sec / 2, 2) as avg_difference_from_schedule_sec
    from avg_segments
    left join segment_geos
        on avg_segments.segment_key = segment_geos.segment_key
    left join routes
        on avg_segments.schedule_b64_url = routes._b64_url
        and avg_segments.schedule_dt = routes.dt
        and avg_segments.route_id = routes.route_id
    -- drop where we don't have a geography since the point here is mapping
    where segment_geos.segment_linestring is not null
)

select * from metric_segment_performance
