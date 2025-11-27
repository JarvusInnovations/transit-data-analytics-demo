{{ config(materialized='table') }}

-- Demo model comparing segment performance between 2023-W41 and 2024-W41
-- Handles the challenge of 2023-W41 spanning two schedule versions by aggregating metrics

with metric_segment_performance as (
    select * from {{ ref('metric_segment_performance') }}
    where feed_name = 'SEPTA Bus Schedule'
    and pick_label in ('2023-W41', '2024-W41')
),

-- Aggregate 2023-W41 data which spans two schedule versions
-- Use simple averages of the metrics (they're already percentages/averages)
-- Cast segment_linestring to STRING since BigQuery can't group by GEOGRAPHY
earlier_period_raw as (
    select
        feed_name,
        route_short_name,
        st_astext(segment_linestring) as segment_linestring,
        hour_type,
        pick_label,
        -- Keep one representative value for identifiers
        -- These may differ across schedule versions but we need something for display
        max(segment_key) as segment_key,
        max(segment_name) as segment_name,
        max(route_id) as route_id,
        max(stop_id) as stop_id,
        max(next_stop_id) as next_stop_id,
        max(shape_id) as shape_id,
        max(schedule_dt) as schedule_dt,
        max(schedule_b64_url) as schedule_b64_url,
        -- Aggregate performance metrics across schedule versions
        avg(pct_early) as pct_early,
        avg(pct_late) as pct_late,
        avg(pct_on_time) as pct_on_time,
        avg(avg_difference_from_schedule_sec) as avg_difference_from_schedule_sec,
        count(*) as schedule_version_count
    from metric_segment_performance
    where pick_label = '2023-W41'
    group by
        feed_name,
        route_short_name,
        st_astext(segment_linestring),
        hour_type,
        pick_label
),

-- 2024-W41 should only have one schedule version per segment
later_period as (
    select
        feed_name,
        route_short_name,
        st_astext(segment_linestring) as segment_linestring,
        hour_type,
        pick_label,
        segment_key,
        segment_name,
        route_id,
        stop_id,
        next_stop_id,
        shape_id,
        schedule_dt,
        schedule_b64_url,
        pct_early,
        pct_late,
        pct_on_time,
        avg_difference_from_schedule_sec
    from metric_segment_performance
    where pick_label = '2024-W41'
),

-- Full outer join on route_short_name + segment_linestring + hour_type
-- This keeps unmatched segments from both periods
demo_segment_performance as (
    select
        -- Use coalesce to get values from whichever side has them
        coalesce(later_period.feed_name, earlier_period_raw.feed_name) as feed_name,
        coalesce(later_period.route_short_name, earlier_period_raw.route_short_name) as route_short_name,
        coalesce(later_period.hour_type, earlier_period_raw.hour_type) as hour_type,

        -- Segment geometry - prefer later period as the "canonical" version
        coalesce(
            later_period.segment_linestring,
            earlier_period_raw.segment_linestring
        ) as segment_linestring,

        -- Match status
        case
            when earlier_period_raw.segment_linestring is null then 'later_only'
            when later_period.segment_linestring is null then 'earlier_only'
            else 'matched'
        end as match_status,

        -- Earlier period (2023-W41) columns
        earlier_period_raw.pick_label as earlier_pick_label,
        earlier_period_raw.segment_key as earlier_segment_key,
        earlier_period_raw.segment_name as earlier_segment_name,
        earlier_period_raw.route_id as earlier_route_id,
        earlier_period_raw.stop_id as earlier_stop_id,
        earlier_period_raw.next_stop_id as earlier_next_stop_id,
        earlier_period_raw.shape_id as earlier_shape_id,
        earlier_period_raw.schedule_dt as earlier_schedule_dt,
        earlier_period_raw.schedule_b64_url as earlier_schedule_b64_url,
        earlier_period_raw.schedule_version_count as earlier_schedule_version_count,
        round(earlier_period_raw.pct_early, 2) as earlier_pct_early,
        round(earlier_period_raw.pct_late, 2) as earlier_pct_late,
        round(earlier_period_raw.pct_on_time, 2) as earlier_pct_on_time,
        round(earlier_period_raw.avg_difference_from_schedule_sec, 2) as earlier_avg_difference_from_schedule_sec,

        -- Later period (2024-W41) columns
        later_period.pick_label as later_pick_label,
        later_period.segment_key as later_segment_key,
        later_period.segment_name as later_segment_name,
        later_period.route_id as later_route_id,
        later_period.stop_id as later_stop_id,
        later_period.next_stop_id as later_next_stop_id,
        later_period.shape_id as later_shape_id,
        later_period.schedule_dt as later_schedule_dt,
        later_period.schedule_b64_url as later_schedule_b64_url,
        round(later_period.pct_early, 2) as later_pct_early,
        round(later_period.pct_late, 2) as later_pct_late,
        round(later_period.pct_on_time, 2) as later_pct_on_time,
        round(later_period.avg_difference_from_schedule_sec, 2) as later_avg_difference_from_schedule_sec,

        -- Change metrics (later - earlier, so positive = increase)
        round(later_period.pct_early - earlier_period_raw.pct_early, 2) as change_pct_early,
        round(later_period.pct_late - earlier_period_raw.pct_late, 2) as change_pct_late,
        round(later_period.pct_on_time - earlier_period_raw.pct_on_time, 2) as change_pct_on_time,
        round(
            later_period.avg_difference_from_schedule_sec - earlier_period_raw.avg_difference_from_schedule_sec,
            2
        ) as change_avg_difference_from_schedule_sec

    from later_period
    full outer join earlier_period_raw
        on later_period.route_short_name = earlier_period_raw.route_short_name
        and later_period.segment_linestring = earlier_period_raw.segment_linestring
        and later_period.hour_type = earlier_period_raw.hour_type
)

select * from demo_segment_performance
