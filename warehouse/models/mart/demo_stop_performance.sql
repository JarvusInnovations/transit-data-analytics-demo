{{ config(materialized='table') }}

-- Demo model comparing stop performance between 2023-W41 and 2024-W41
-- Handles the challenge of 2023-W41 spanning two schedule versions by aggregating metrics
-- Uses two-pass matching: exact point match first, then geographic fallback

with metric_stop_performance as (
    select * from {{ ref('metric_stop_performance_all_shapes') }}
    where feed_name = 'SEPTA Bus Schedule'
    and pick_label in ('2023-W41', '2024-W41')
),

-- Aggregate 2023-W41 data which spans two schedule versions
earlier_period as (
    select
        feed_name,
        st_astext(stop_pt) as stop_pt_text,
        any_value(stop_pt) as stop_pt_geo,
        hour_type,
        pick_label,
        max(stop_key) as stop_key,
        max(stop_name) as stop_name,
        max(stop_id) as stop_id,
        max(schedule_dt) as schedule_dt,
        max(schedule_b64_url) as schedule_b64_url,
        avg(ct_observations) as ct_observations,
        avg(pct_early) as pct_early,
        avg(pct_late) as pct_late,
        avg(pct_on_time) as pct_on_time,
        avg(median_difference_from_schedule_sec) as median_difference_from_schedule_sec,
        avg(p25_difference_from_schedule_sec) as p25_difference_from_schedule_sec,
        avg(p75_difference_from_schedule_sec) as p75_difference_from_schedule_sec,
        avg(avg_difference_from_schedule_sec) as avg_difference_from_schedule_sec,
        count(*) as schedule_version_count
    from metric_stop_performance
    where pick_label = '2023-W41'
    group by
        feed_name,
        st_astext(stop_pt),
        hour_type,
        pick_label
),

-- 2024-W41 should only have one schedule version per stop
later_period as (
    select
        feed_name,
        st_astext(stop_pt) as stop_pt_text,
        stop_pt as stop_pt_geo,
        hour_type,
        pick_label,
        stop_key,
        stop_name,
        stop_id,
        schedule_dt,
        schedule_b64_url,
        ct_observations,
        pct_early,
        pct_late,
        pct_on_time,
        median_difference_from_schedule_sec,
        p25_difference_from_schedule_sec,
        p75_difference_from_schedule_sec,
        avg_difference_from_schedule_sec
    from metric_stop_performance
    where pick_label = '2024-W41'
),

-- First pass: exact point text match
exact_matches as (
    select
        later_period.feed_name,
        later_period.hour_type,
        later_period.stop_pt_text as stop_pt,
        'matched' as match_status,
        'exact' as match_type,
        earlier_period.pick_label as earlier_pick_label,
        earlier_period.stop_key as earlier_stop_key,
        earlier_period.stop_name as earlier_stop_name,
        earlier_period.stop_id as earlier_stop_id,
        earlier_period.schedule_dt as earlier_schedule_dt,
        earlier_period.schedule_b64_url as earlier_schedule_b64_url,
        earlier_period.schedule_version_count as earlier_schedule_version_count,
        earlier_period.ct_observations as earlier_ct_observations,
        earlier_period.pct_early as earlier_pct_early,
        earlier_period.pct_late as earlier_pct_late,
        earlier_period.pct_on_time as earlier_pct_on_time,
        earlier_period.median_difference_from_schedule_sec as earlier_median_difference_from_schedule_sec,
        earlier_period.p25_difference_from_schedule_sec as earlier_p25_difference_from_schedule_sec,
        earlier_period.p75_difference_from_schedule_sec as earlier_p75_difference_from_schedule_sec,
        earlier_period.avg_difference_from_schedule_sec as earlier_avg_difference_from_schedule_sec,
        later_period.pick_label as later_pick_label,
        later_period.stop_key as later_stop_key,
        later_period.stop_name as later_stop_name,
        later_period.stop_id as later_stop_id,
        later_period.schedule_dt as later_schedule_dt,
        later_period.schedule_b64_url as later_schedule_b64_url,
        later_period.ct_observations as later_ct_observations,
        later_period.pct_early as later_pct_early,
        later_period.pct_late as later_pct_late,
        later_period.pct_on_time as later_pct_on_time,
        later_period.median_difference_from_schedule_sec as later_median_difference_from_schedule_sec,
        later_period.p25_difference_from_schedule_sec as later_p25_difference_from_schedule_sec,
        later_period.p75_difference_from_schedule_sec as later_p75_difference_from_schedule_sec,
        later_period.avg_difference_from_schedule_sec as later_avg_difference_from_schedule_sec
    from later_period
    inner join earlier_period
        on later_period.stop_pt_text = earlier_period.stop_pt_text
        and later_period.hour_type = earlier_period.hour_type
),

-- Identify unmatched stops from each period
later_unmatched as (
    select later_period.*
    from later_period
    left join exact_matches
        on later_period.stop_pt_text = exact_matches.stop_pt
        and later_period.hour_type = exact_matches.hour_type
    where exact_matches.stop_pt is null
),

earlier_unmatched as (
    select earlier_period.*
    from earlier_period
    left join exact_matches
        on earlier_period.stop_pt_text = exact_matches.stop_pt
        and earlier_period.hour_type = exact_matches.hour_type
    where exact_matches.stop_pt is null
),

-- Second pass: geographic match (stops within 50m)
geo_match_candidates as (
    select
        later_unmatched.feed_name,
        later_unmatched.hour_type,
        later_unmatched.stop_pt_text as stop_pt,
        'matched' as match_status,
        'geographic' as match_type,
        earlier_unmatched.pick_label as earlier_pick_label,
        earlier_unmatched.stop_key as earlier_stop_key,
        earlier_unmatched.stop_name as earlier_stop_name,
        earlier_unmatched.stop_id as earlier_stop_id,
        earlier_unmatched.schedule_dt as earlier_schedule_dt,
        earlier_unmatched.schedule_b64_url as earlier_schedule_b64_url,
        earlier_unmatched.schedule_version_count as earlier_schedule_version_count,
        earlier_unmatched.ct_observations as earlier_ct_observations,
        earlier_unmatched.pct_early as earlier_pct_early,
        earlier_unmatched.pct_late as earlier_pct_late,
        earlier_unmatched.pct_on_time as earlier_pct_on_time,
        earlier_unmatched.median_difference_from_schedule_sec as earlier_median_difference_from_schedule_sec,
        earlier_unmatched.p25_difference_from_schedule_sec as earlier_p25_difference_from_schedule_sec,
        earlier_unmatched.p75_difference_from_schedule_sec as earlier_p75_difference_from_schedule_sec,
        earlier_unmatched.avg_difference_from_schedule_sec as earlier_avg_difference_from_schedule_sec,
        later_unmatched.pick_label as later_pick_label,
        later_unmatched.stop_key as later_stop_key,
        later_unmatched.stop_name as later_stop_name,
        later_unmatched.stop_id as later_stop_id,
        later_unmatched.schedule_dt as later_schedule_dt,
        later_unmatched.schedule_b64_url as later_schedule_b64_url,
        later_unmatched.ct_observations as later_ct_observations,
        later_unmatched.pct_early as later_pct_early,
        later_unmatched.pct_late as later_pct_late,
        later_unmatched.pct_on_time as later_pct_on_time,
        later_unmatched.median_difference_from_schedule_sec as later_median_difference_from_schedule_sec,
        later_unmatched.p25_difference_from_schedule_sec as later_p25_difference_from_schedule_sec,
        later_unmatched.p75_difference_from_schedule_sec as later_p75_difference_from_schedule_sec,
        later_unmatched.avg_difference_from_schedule_sec as later_avg_difference_from_schedule_sec,
        st_distance(later_unmatched.stop_pt_geo, earlier_unmatched.stop_pt_geo) as distance_m,
        row_number() over (
            partition by later_unmatched.stop_pt_text, later_unmatched.hour_type
            order by st_distance(later_unmatched.stop_pt_geo, earlier_unmatched.stop_pt_geo)
        ) as later_rank,
        row_number() over (
            partition by earlier_unmatched.stop_pt_text, earlier_unmatched.hour_type
            order by st_distance(later_unmatched.stop_pt_geo, earlier_unmatched.stop_pt_geo)
        ) as earlier_rank
    from later_unmatched
    inner join earlier_unmatched
        on later_unmatched.hour_type = earlier_unmatched.hour_type
        and st_distance(later_unmatched.stop_pt_geo, earlier_unmatched.stop_pt_geo) < 30
),

-- Deduplicated geo matches - each stop can only match once (best match wins)
geo_matches as (
    select * except (distance_m, later_rank, earlier_rank)
    from geo_match_candidates
    where later_rank = 1 and earlier_rank = 1
),

-- Track which earlier stops were used in geo matches
earlier_used_in_geo as (
    select distinct earlier_stop_key, hour_type
    from geo_matches
),

-- Later stops still unmatched after geo matching
later_still_unmatched as (
    select later_unmatched.*
    from later_unmatched
    left join geo_matches
        on later_unmatched.stop_pt_text = geo_matches.stop_pt
        and later_unmatched.hour_type = geo_matches.hour_type
    where geo_matches.stop_pt is null
),

-- Earlier stops still unmatched after geo matching
earlier_still_unmatched as (
    select earlier_unmatched.*
    from earlier_unmatched
    left join earlier_used_in_geo
        on earlier_unmatched.stop_key = earlier_used_in_geo.earlier_stop_key
        and earlier_unmatched.hour_type = earlier_used_in_geo.hour_type
    where earlier_used_in_geo.earlier_stop_key is null
),

-- Later-only records (no match found)
later_only as (
    select
        feed_name,
        hour_type,
        stop_pt_text as stop_pt,
        'later_only' as match_status,
        cast(null as string) as match_type,
        cast(null as string) as earlier_pick_label,
        cast(null as string) as earlier_stop_key,
        cast(null as string) as earlier_stop_name,
        cast(null as string) as earlier_stop_id,
        cast(null as date) as earlier_schedule_dt,
        cast(null as string) as earlier_schedule_b64_url,
        cast(null as int64) as earlier_schedule_version_count,
        cast(null as float64) as earlier_ct_observations,
        cast(null as float64) as earlier_pct_early,
        cast(null as float64) as earlier_pct_late,
        cast(null as float64) as earlier_pct_on_time,
        cast(null as float64) as earlier_median_difference_from_schedule_sec,
        cast(null as float64) as earlier_p25_difference_from_schedule_sec,
        cast(null as float64) as earlier_p75_difference_from_schedule_sec,
        cast(null as float64) as earlier_avg_difference_from_schedule_sec,
        pick_label as later_pick_label,
        stop_key as later_stop_key,
        stop_name as later_stop_name,
        stop_id as later_stop_id,
        schedule_dt as later_schedule_dt,
        schedule_b64_url as later_schedule_b64_url,
        ct_observations as later_ct_observations,
        pct_early as later_pct_early,
        pct_late as later_pct_late,
        pct_on_time as later_pct_on_time,
        median_difference_from_schedule_sec as later_median_difference_from_schedule_sec,
        p25_difference_from_schedule_sec as later_p25_difference_from_schedule_sec,
        p75_difference_from_schedule_sec as later_p75_difference_from_schedule_sec,
        avg_difference_from_schedule_sec as later_avg_difference_from_schedule_sec
    from later_still_unmatched
),

-- Earlier-only records (no match found)
earlier_only as (
    select
        feed_name,
        hour_type,
        stop_pt_text as stop_pt,
        'earlier_only' as match_status,
        cast(null as string) as match_type,
        pick_label as earlier_pick_label,
        stop_key as earlier_stop_key,
        stop_name as earlier_stop_name,
        stop_id as earlier_stop_id,
        schedule_dt as earlier_schedule_dt,
        schedule_b64_url as earlier_schedule_b64_url,
        schedule_version_count as earlier_schedule_version_count,
        ct_observations as earlier_ct_observations,
        pct_early as earlier_pct_early,
        pct_late as earlier_pct_late,
        pct_on_time as earlier_pct_on_time,
        median_difference_from_schedule_sec as earlier_median_difference_from_schedule_sec,
        p25_difference_from_schedule_sec as earlier_p25_difference_from_schedule_sec,
        p75_difference_from_schedule_sec as earlier_p75_difference_from_schedule_sec,
        avg_difference_from_schedule_sec as earlier_avg_difference_from_schedule_sec,
        cast(null as string) as later_pick_label,
        cast(null as string) as later_stop_key,
        cast(null as string) as later_stop_name,
        cast(null as string) as later_stop_id,
        cast(null as date) as later_schedule_dt,
        cast(null as string) as later_schedule_b64_url,
        cast(null as float64) as later_ct_observations,
        cast(null as float64) as later_pct_early,
        cast(null as float64) as later_pct_late,
        cast(null as float64) as later_pct_on_time,
        cast(null as float64) as later_median_difference_from_schedule_sec,
        cast(null as float64) as later_p25_difference_from_schedule_sec,
        cast(null as float64) as later_p75_difference_from_schedule_sec,
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
demo_stop_performance as (
    select
        feed_name,
        hour_type,
        stop_pt,
        match_status,
        match_type,
        earlier_pick_label,
        earlier_stop_key,
        earlier_stop_name,
        earlier_stop_id,
        earlier_schedule_dt,
        earlier_schedule_b64_url,
        earlier_schedule_version_count,
        round(earlier_ct_observations, 0) as earlier_ct_observations,
        round(earlier_pct_early, 2) as earlier_pct_early,
        round(earlier_pct_late, 2) as earlier_pct_late,
        round(earlier_pct_on_time, 2) as earlier_pct_on_time,
        round(earlier_median_difference_from_schedule_sec, 2) as earlier_median_difference_from_schedule_sec,
        round(earlier_p25_difference_from_schedule_sec, 2) as earlier_p25_difference_from_schedule_sec,
        round(earlier_p75_difference_from_schedule_sec, 2) as earlier_p75_difference_from_schedule_sec,
        round(earlier_avg_difference_from_schedule_sec, 2) as earlier_avg_difference_from_schedule_sec,
        later_pick_label,
        later_stop_key,
        later_stop_name,
        later_stop_id,
        later_schedule_dt,
        later_schedule_b64_url,
        round(later_ct_observations, 0) as later_ct_observations,
        round(later_pct_early, 2) as later_pct_early,
        round(later_pct_late, 2) as later_pct_late,
        round(later_pct_on_time, 2) as later_pct_on_time,
        round(later_median_difference_from_schedule_sec, 2) as later_median_difference_from_schedule_sec,
        round(later_p25_difference_from_schedule_sec, 2) as later_p25_difference_from_schedule_sec,
        round(later_p75_difference_from_schedule_sec, 2) as later_p75_difference_from_schedule_sec,
        round(later_avg_difference_from_schedule_sec, 2) as later_avg_difference_from_schedule_sec,
        round(later_pct_early - earlier_pct_early, 2) as change_pct_early,
        round(later_pct_late - earlier_pct_late, 2) as change_pct_late,
        round(later_pct_on_time - earlier_pct_on_time, 2) as change_pct_on_time,
        round(later_avg_difference_from_schedule_sec - earlier_avg_difference_from_schedule_sec, 2) as change_avg_difference_from_schedule_sec
    from combined
    -- Filter out invalid on-time percentages (null, 0, or >= 100)
    where (match_status = 'matched'
           and earlier_pct_on_time is not null and earlier_pct_on_time > 0 and earlier_pct_on_time < 100
           and later_pct_on_time is not null and later_pct_on_time > 0 and later_pct_on_time < 100)
       or (match_status = 'earlier_only'
           and earlier_pct_on_time is not null and earlier_pct_on_time > 0 and earlier_pct_on_time < 100)
       or (match_status = 'later_only'
           and later_pct_on_time is not null and later_pct_on_time > 0 and later_pct_on_time < 100)
),

-- Aggregate across hour types to create synthetic "all" records
all_hours as (
    select
        feed_name,
        'all' as hour_type,
        stop_pt,
        match_status,
        cast(null as string) as match_type,
        -- Earlier period: use values from any row (same across hour_types)
        max(earlier_pick_label) as earlier_pick_label,
        max(earlier_stop_key) as earlier_stop_key,
        max(earlier_stop_name) as earlier_stop_name,
        max(earlier_stop_id) as earlier_stop_id,
        max(earlier_schedule_dt) as earlier_schedule_dt,
        max(earlier_schedule_b64_url) as earlier_schedule_b64_url,
        max(earlier_schedule_version_count) as earlier_schedule_version_count,
        -- Weighted metrics for earlier period
        round(sum(earlier_ct_observations), 0) as earlier_ct_observations,
        round(sum(earlier_pct_early * earlier_ct_observations) / nullif(sum(earlier_ct_observations), 0), 2) as earlier_pct_early,
        round(sum(earlier_pct_late * earlier_ct_observations) / nullif(sum(earlier_ct_observations), 0), 2) as earlier_pct_late,
        round(sum(earlier_pct_on_time * earlier_ct_observations) / nullif(sum(earlier_ct_observations), 0), 2) as earlier_pct_on_time,
        round(sum(earlier_median_difference_from_schedule_sec * earlier_ct_observations) / nullif(sum(earlier_ct_observations), 0), 2) as earlier_median_difference_from_schedule_sec,
        round(sum(earlier_p25_difference_from_schedule_sec * earlier_ct_observations) / nullif(sum(earlier_ct_observations), 0), 2) as earlier_p25_difference_from_schedule_sec,
        round(sum(earlier_p75_difference_from_schedule_sec * earlier_ct_observations) / nullif(sum(earlier_ct_observations), 0), 2) as earlier_p75_difference_from_schedule_sec,
        round(sum(earlier_avg_difference_from_schedule_sec * earlier_ct_observations) / nullif(sum(earlier_ct_observations), 0), 2) as earlier_avg_difference_from_schedule_sec,
        -- Later period: use values from any row
        max(later_pick_label) as later_pick_label,
        max(later_stop_key) as later_stop_key,
        max(later_stop_name) as later_stop_name,
        max(later_stop_id) as later_stop_id,
        max(later_schedule_dt) as later_schedule_dt,
        max(later_schedule_b64_url) as later_schedule_b64_url,
        -- Weighted metrics for later period
        round(sum(later_ct_observations), 0) as later_ct_observations,
        round(sum(later_pct_early * later_ct_observations) / nullif(sum(later_ct_observations), 0), 2) as later_pct_early,
        round(sum(later_pct_late * later_ct_observations) / nullif(sum(later_ct_observations), 0), 2) as later_pct_late,
        round(sum(later_pct_on_time * later_ct_observations) / nullif(sum(later_ct_observations), 0), 2) as later_pct_on_time,
        round(sum(later_median_difference_from_schedule_sec * later_ct_observations) / nullif(sum(later_ct_observations), 0), 2) as later_median_difference_from_schedule_sec,
        round(sum(later_p25_difference_from_schedule_sec * later_ct_observations) / nullif(sum(later_ct_observations), 0), 2) as later_p25_difference_from_schedule_sec,
        round(sum(later_p75_difference_from_schedule_sec * later_ct_observations) / nullif(sum(later_ct_observations), 0), 2) as later_p75_difference_from_schedule_sec,
        round(sum(later_avg_difference_from_schedule_sec * later_ct_observations) / nullif(sum(later_ct_observations), 0), 2) as later_avg_difference_from_schedule_sec,
        -- Change metrics computed from weighted averages
        round(
            sum(later_pct_early * later_ct_observations) / nullif(sum(later_ct_observations), 0)
            - sum(earlier_pct_early * earlier_ct_observations) / nullif(sum(earlier_ct_observations), 0)
        , 2) as change_pct_early,
        round(
            sum(later_pct_late * later_ct_observations) / nullif(sum(later_ct_observations), 0)
            - sum(earlier_pct_late * earlier_ct_observations) / nullif(sum(earlier_ct_observations), 0)
        , 2) as change_pct_late,
        round(
            sum(later_pct_on_time * later_ct_observations) / nullif(sum(later_ct_observations), 0)
            - sum(earlier_pct_on_time * earlier_ct_observations) / nullif(sum(earlier_ct_observations), 0)
        , 2) as change_pct_on_time,
        round(
            sum(later_avg_difference_from_schedule_sec * later_ct_observations) / nullif(sum(later_ct_observations), 0)
            - sum(earlier_avg_difference_from_schedule_sec * earlier_ct_observations) / nullif(sum(earlier_ct_observations), 0)
        , 2) as change_avg_difference_from_schedule_sec
    from demo_stop_performance
    where match_status = 'matched'
    group by feed_name, stop_pt, match_status
)

select * from demo_stop_performance
union all
select * from all_hours
