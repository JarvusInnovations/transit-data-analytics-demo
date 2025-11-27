{{ config(materialized='table') }}

-- Demo model comparing segment performance between 2023-W41 and 2024-W41
-- Handles the challenge of 2023-W41 spanning two schedule versions by aggregating metrics
-- Uses two-pass matching: exact linestring match first, then geographic fallback using endpoint distance

with metric_segment_performance as (
    select * from {{ ref('metric_segment_performance') }}
    where feed_name = 'SEPTA Bus Schedule'
    and pick_label in ('2023-W41', '2024-W41')
),

-- Aggregate 2023-W41 data which spans two schedule versions
earlier_period as (
    select
        feed_name,
        route_short_name,
        st_astext(segment_linestring) as segment_linestring_text,
        any_value(segment_linestring) as segment_linestring_geo,
        hour_type,
        pick_label,
        max(segment_key) as segment_key,
        max(segment_name) as segment_name,
        max(route_id) as route_id,
        max(stop_id) as stop_id,
        max(next_stop_id) as next_stop_id,
        max(shape_id) as shape_id,
        max(schedule_dt) as schedule_dt,
        max(schedule_b64_url) as schedule_b64_url,
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
        st_astext(segment_linestring) as segment_linestring_text,
        segment_linestring as segment_linestring_geo,
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

-- First pass: exact linestring text match
exact_matches as (
    select
        later_period.feed_name,
        later_period.route_short_name,
        later_period.hour_type,
        later_period.segment_linestring_text as segment_linestring,
        'matched' as match_status,
        'exact' as match_type,
        earlier_period.pick_label as earlier_pick_label,
        earlier_period.segment_key as earlier_segment_key,
        earlier_period.segment_name as earlier_segment_name,
        earlier_period.route_id as earlier_route_id,
        earlier_period.stop_id as earlier_stop_id,
        earlier_period.next_stop_id as earlier_next_stop_id,
        earlier_period.shape_id as earlier_shape_id,
        earlier_period.schedule_dt as earlier_schedule_dt,
        earlier_period.schedule_b64_url as earlier_schedule_b64_url,
        earlier_period.schedule_version_count as earlier_schedule_version_count,
        earlier_period.pct_early as earlier_pct_early,
        earlier_period.pct_late as earlier_pct_late,
        earlier_period.pct_on_time as earlier_pct_on_time,
        earlier_period.avg_difference_from_schedule_sec as earlier_avg_difference_from_schedule_sec,
        later_period.pick_label as later_pick_label,
        later_period.segment_key as later_segment_key,
        later_period.segment_name as later_segment_name,
        later_period.route_id as later_route_id,
        later_period.stop_id as later_stop_id,
        later_period.next_stop_id as later_next_stop_id,
        later_period.shape_id as later_shape_id,
        later_period.schedule_dt as later_schedule_dt,
        later_period.schedule_b64_url as later_schedule_b64_url,
        later_period.pct_early as later_pct_early,
        later_period.pct_late as later_pct_late,
        later_period.pct_on_time as later_pct_on_time,
        later_period.avg_difference_from_schedule_sec as later_avg_difference_from_schedule_sec
    from later_period
    inner join earlier_period
        on later_period.route_short_name = earlier_period.route_short_name
        and later_period.segment_linestring_text = earlier_period.segment_linestring_text
        and later_period.hour_type = earlier_period.hour_type
),

-- Identify unmatched segments from each period
later_unmatched as (
    select later_period.*
    from later_period
    left join exact_matches
        on later_period.route_short_name = exact_matches.route_short_name
        and later_period.segment_linestring_text = exact_matches.segment_linestring
        and later_period.hour_type = exact_matches.hour_type
    where exact_matches.segment_linestring is null
),

earlier_unmatched as (
    select earlier_period.*
    from earlier_period
    left join exact_matches
        on earlier_period.route_short_name = exact_matches.route_short_name
        and earlier_period.segment_linestring_text = exact_matches.segment_linestring
        and earlier_period.hour_type = exact_matches.hour_type
    where exact_matches.segment_linestring is null
),

-- Second pass: geographic match using endpoint distance (max 50m threshold)
-- Match segments where both start and end points are within 50m
geo_match_candidates as (
    select
        later_unmatched.feed_name,
        later_unmatched.route_short_name,
        later_unmatched.hour_type,
        later_unmatched.segment_linestring_text as segment_linestring,
        'matched' as match_status,
        'geographic' as match_type,
        earlier_unmatched.pick_label as earlier_pick_label,
        earlier_unmatched.segment_key as earlier_segment_key,
        earlier_unmatched.segment_name as earlier_segment_name,
        earlier_unmatched.route_id as earlier_route_id,
        earlier_unmatched.stop_id as earlier_stop_id,
        earlier_unmatched.next_stop_id as earlier_next_stop_id,
        earlier_unmatched.shape_id as earlier_shape_id,
        earlier_unmatched.schedule_dt as earlier_schedule_dt,
        earlier_unmatched.schedule_b64_url as earlier_schedule_b64_url,
        earlier_unmatched.schedule_version_count as earlier_schedule_version_count,
        earlier_unmatched.pct_early as earlier_pct_early,
        earlier_unmatched.pct_late as earlier_pct_late,
        earlier_unmatched.pct_on_time as earlier_pct_on_time,
        earlier_unmatched.avg_difference_from_schedule_sec as earlier_avg_difference_from_schedule_sec,
        later_unmatched.pick_label as later_pick_label,
        later_unmatched.segment_key as later_segment_key,
        later_unmatched.segment_name as later_segment_name,
        later_unmatched.route_id as later_route_id,
        later_unmatched.stop_id as later_stop_id,
        later_unmatched.next_stop_id as later_next_stop_id,
        later_unmatched.shape_id as later_shape_id,
        later_unmatched.schedule_dt as later_schedule_dt,
        later_unmatched.schedule_b64_url as later_schedule_b64_url,
        later_unmatched.pct_early as later_pct_early,
        later_unmatched.pct_late as later_pct_late,
        later_unmatched.pct_on_time as later_pct_on_time,
        later_unmatched.avg_difference_from_schedule_sec as later_avg_difference_from_schedule_sec,
        -- Calculate max endpoint distance for ranking
        greatest(
            st_distance(
                st_startpoint(later_unmatched.segment_linestring_geo),
                st_startpoint(earlier_unmatched.segment_linestring_geo)
            ),
            st_distance(
                st_endpoint(later_unmatched.segment_linestring_geo),
                st_endpoint(earlier_unmatched.segment_linestring_geo)
            )
        ) as max_endpoint_dist_m,
        -- Rank matches for each later segment (pick closest)
        row_number() over (
            partition by
                later_unmatched.route_short_name,
                later_unmatched.segment_linestring_text,
                later_unmatched.hour_type
            order by greatest(
                st_distance(
                    st_startpoint(later_unmatched.segment_linestring_geo),
                    st_startpoint(earlier_unmatched.segment_linestring_geo)
                ),
                st_distance(
                    st_endpoint(later_unmatched.segment_linestring_geo),
                    st_endpoint(earlier_unmatched.segment_linestring_geo)
                )
            )
        ) as later_rank,
        -- Rank matches for each earlier segment (pick closest)
        row_number() over (
            partition by
                earlier_unmatched.route_short_name,
                earlier_unmatched.segment_linestring_text,
                earlier_unmatched.hour_type
            order by greatest(
                st_distance(
                    st_startpoint(later_unmatched.segment_linestring_geo),
                    st_startpoint(earlier_unmatched.segment_linestring_geo)
                ),
                st_distance(
                    st_endpoint(later_unmatched.segment_linestring_geo),
                    st_endpoint(earlier_unmatched.segment_linestring_geo)
                )
            )
        ) as earlier_rank
    from later_unmatched
    inner join earlier_unmatched
        on later_unmatched.route_short_name = earlier_unmatched.route_short_name
        and later_unmatched.hour_type = earlier_unmatched.hour_type
        -- Both endpoints must be within 50m
        and st_distance(
            st_startpoint(later_unmatched.segment_linestring_geo),
            st_startpoint(earlier_unmatched.segment_linestring_geo)
        ) < 50
        and st_distance(
            st_endpoint(later_unmatched.segment_linestring_geo),
            st_endpoint(earlier_unmatched.segment_linestring_geo)
        ) < 50
),

-- Deduplicated geo matches - each segment can only match once (best match wins)
geo_matches as (
    select * except (max_endpoint_dist_m, later_rank, earlier_rank)
    from geo_match_candidates
    where later_rank = 1 and earlier_rank = 1
),

-- Track which earlier segments were used in geo matches
earlier_used_in_geo as (
    select distinct earlier_segment_key, hour_type
    from geo_matches
),

-- Later segments still unmatched after geo matching
later_still_unmatched as (
    select later_unmatched.*
    from later_unmatched
    left join geo_matches
        on later_unmatched.route_short_name = geo_matches.route_short_name
        and later_unmatched.segment_linestring_text = geo_matches.segment_linestring
        and later_unmatched.hour_type = geo_matches.hour_type
    where geo_matches.segment_linestring is null
),

-- Earlier segments still unmatched after geo matching
earlier_still_unmatched as (
    select earlier_unmatched.*
    from earlier_unmatched
    left join earlier_used_in_geo
        on earlier_unmatched.segment_key = earlier_used_in_geo.earlier_segment_key
        and earlier_unmatched.hour_type = earlier_used_in_geo.hour_type
    where earlier_used_in_geo.earlier_segment_key is null
),

-- Later-only records (no match found)
later_only as (
    select
        feed_name,
        route_short_name,
        hour_type,
        segment_linestring_text as segment_linestring,
        'later_only' as match_status,
        cast(null as string) as match_type,
        cast(null as string) as earlier_pick_label,
        cast(null as string) as earlier_segment_key,
        cast(null as string) as earlier_segment_name,
        cast(null as string) as earlier_route_id,
        cast(null as string) as earlier_stop_id,
        cast(null as string) as earlier_next_stop_id,
        cast(null as string) as earlier_shape_id,
        cast(null as date) as earlier_schedule_dt,
        cast(null as string) as earlier_schedule_b64_url,
        cast(null as int64) as earlier_schedule_version_count,
        cast(null as float64) as earlier_pct_early,
        cast(null as float64) as earlier_pct_late,
        cast(null as float64) as earlier_pct_on_time,
        cast(null as float64) as earlier_avg_difference_from_schedule_sec,
        pick_label as later_pick_label,
        segment_key as later_segment_key,
        segment_name as later_segment_name,
        route_id as later_route_id,
        stop_id as later_stop_id,
        next_stop_id as later_next_stop_id,
        shape_id as later_shape_id,
        schedule_dt as later_schedule_dt,
        schedule_b64_url as later_schedule_b64_url,
        pct_early as later_pct_early,
        pct_late as later_pct_late,
        pct_on_time as later_pct_on_time,
        avg_difference_from_schedule_sec as later_avg_difference_from_schedule_sec
    from later_still_unmatched
),

-- Earlier-only records (no match found)
earlier_only as (
    select
        feed_name,
        route_short_name,
        hour_type,
        segment_linestring_text as segment_linestring,
        'earlier_only' as match_status,
        cast(null as string) as match_type,
        pick_label as earlier_pick_label,
        segment_key as earlier_segment_key,
        segment_name as earlier_segment_name,
        route_id as earlier_route_id,
        stop_id as earlier_stop_id,
        next_stop_id as earlier_next_stop_id,
        shape_id as earlier_shape_id,
        schedule_dt as earlier_schedule_dt,
        schedule_b64_url as earlier_schedule_b64_url,
        schedule_version_count as earlier_schedule_version_count,
        pct_early as earlier_pct_early,
        pct_late as earlier_pct_late,
        pct_on_time as earlier_pct_on_time,
        avg_difference_from_schedule_sec as earlier_avg_difference_from_schedule_sec,
        cast(null as string) as later_pick_label,
        cast(null as string) as later_segment_key,
        cast(null as string) as later_segment_name,
        cast(null as string) as later_route_id,
        cast(null as string) as later_stop_id,
        cast(null as string) as later_next_stop_id,
        cast(null as string) as later_shape_id,
        cast(null as date) as later_schedule_dt,
        cast(null as string) as later_schedule_b64_url,
        cast(null as float64) as later_pct_early,
        cast(null as float64) as later_pct_late,
        cast(null as float64) as later_pct_on_time,
        cast(null as float64) as later_avg_difference_from_schedule_sec
    from earlier_still_unmatched
),

-- Combine all results
combined as (
    select * from exact_matches
    union all
    select * from geo_matches
    union all
    select * from later_only
    union all
    select * from earlier_only
),

-- Final output with change metrics
demo_segment_performance as (
    select
        feed_name,
        route_short_name,
        hour_type,
        segment_linestring,
        match_status,
        match_type,
        earlier_pick_label,
        earlier_segment_key,
        earlier_segment_name,
        earlier_route_id,
        earlier_stop_id,
        earlier_next_stop_id,
        earlier_shape_id,
        earlier_schedule_dt,
        earlier_schedule_b64_url,
        earlier_schedule_version_count,
        round(earlier_pct_early, 2) as earlier_pct_early,
        round(earlier_pct_late, 2) as earlier_pct_late,
        round(earlier_pct_on_time, 2) as earlier_pct_on_time,
        round(earlier_avg_difference_from_schedule_sec, 2) as earlier_avg_difference_from_schedule_sec,
        later_pick_label,
        later_segment_key,
        later_segment_name,
        later_route_id,
        later_stop_id,
        later_next_stop_id,
        later_shape_id,
        later_schedule_dt,
        later_schedule_b64_url,
        round(later_pct_early, 2) as later_pct_early,
        round(later_pct_late, 2) as later_pct_late,
        round(later_pct_on_time, 2) as later_pct_on_time,
        round(later_avg_difference_from_schedule_sec, 2) as later_avg_difference_from_schedule_sec,
        round(later_pct_early - earlier_pct_early, 2) as change_pct_early,
        round(later_pct_late - earlier_pct_late, 2) as change_pct_late,
        round(later_pct_on_time - earlier_pct_on_time, 2) as change_pct_on_time,
        round(later_avg_difference_from_schedule_sec - earlier_avg_difference_from_schedule_sec, 2) as change_avg_difference_from_schedule_sec
    from combined
)

select * from demo_segment_performance
