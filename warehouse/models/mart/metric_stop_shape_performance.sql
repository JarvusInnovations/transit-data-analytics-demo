{{ config(materialized='table') }}

with stop_times as (
    select * from {{ ref('fct_observed_stop_times') }}
),

stops as (
    select * from {{ ref('dim_stops') }}
),

perf as (
    select distinct
        feed_key,
        schedule_b64_url,
        schedule_dt,
        stop_key,
        feed_name,
        shape_id,
        pick_label,
        stop_id,
        hour_type,
        count(stop_performance_type) {{ metric_stop_shape_performance_window_over() }} as ct_observations,
        round((countif(stop_performance_type = 'early') {{ metric_stop_shape_performance_window_over() }} --noqa: LT02
            / count(stop_performance_type) {{ metric_stop_shape_performance_window_over() }}) * 100, 2) as pct_early, --noqa: LT02
        round((countif(stop_performance_type = 'late') {{ metric_stop_shape_performance_window_over() }} --noqa: LT02
            / count(stop_performance_type) {{ metric_stop_shape_performance_window_over() }}) * 100, 2) as pct_late, --noqa: LT02
        round((countif(stop_performance_type = 'on_time') {{ metric_stop_shape_performance_window_over() }} --noqa: LT02
            / count(stop_performance_type) {{ metric_stop_shape_performance_window_over() }}) * 100, 2) as pct_on_time, --noqa: LT02
        percentile_disc(diff_from_schedule, .5) {{ metric_stop_shape_performance_window_over() }} as median_difference_from_schedule_sec,
        percentile_disc(diff_from_schedule, .25) {{ metric_stop_shape_performance_window_over() }} as p25_difference_from_schedule_sec,
        percentile_disc(diff_from_schedule, .75) {{ metric_stop_shape_performance_window_over() }} as p75_difference_from_schedule_sec,
        avg(diff_from_schedule) {{ metric_stop_shape_performance_window_over() }} as avg_difference_from_schedule_sec
    from stop_times
),

metric_stop_performance_all_shapes as (
    select
        perf.feed_key,
        perf.schedule_b64_url,
        perf.schedule_dt,
        perf.stop_key,
        perf.feed_name,
        perf.shape_id,
        perf.pick_label,
        perf.stop_id,
        perf.hour_type,
        stops.stop_pt,
        perf.ct_observations,
        perf.pct_early,
        perf.pct_late,
        perf.pct_on_time,
        perf.median_difference_from_schedule_sec,
        perf.p25_difference_from_schedule_sec,
        perf.p75_difference_from_schedule_sec,
        perf.avg_difference_from_schedule_sec
    from perf
    left join stops
        on perf.stop_key = stops.stop_key
)

select * from metric_stop_performance_all_shapes
