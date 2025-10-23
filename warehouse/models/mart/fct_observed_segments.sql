{{ config(materialized='table') }}

with stop_times as (
    select * from {{ ref('fct_observed_stop_times') }}
),

shapes as (
    select * from {{ ref('dim_shapes') }}
),

make_segments as (
    select distinct
        schedule_b64_url,
        schedule_dt,
        stop_id,
        shape_id,
        shape_closest_point_to_stop_as_pct,
        lead(shape_closest_point_to_stop_as_pct) over (partition by schedule_b64_url, trip_id, service_date order by scheduled_stop_sequence) as next_stop_pct,
        lead(shape_id) over (partition by schedule_b64_url, trip_id, service_date order by scheduled_stop_sequence) as next_stop_id
    from stop_times
),

fct_observed_segments as (
    select --noqa: ST06
        make_segments.shape_id || "-" || make_segments.next_stop_id as segment_id,
        make_segments.schedule_b64_url,
        make_segments.schedule_dt,
        make_segments.stop_id,
        make_segments.shape_id,
        make_segments.shape_closest_point_to_stop_as_pct,
        make_segments.next_stop_pct,
        make_segments.next_stop_id,
        st_linesubstring(shapes.shape_linestring, make_segments.shape_closest_point_to_stop_as_pct, make_segments.next_stop_pct) as segment_linestring
    from make_segments
    left join shapes
        on
            make_segments.schedule_b64_url = shapes._b64_url
            and make_segments.schedule_dt = shapes._valid_from
            and make_segments.shape_id = shapes.shape_id
    where make_segments.shape_closest_point_to_stop_as_pct < make_segments.next_stop_pct
)

select * from fct_observed_segments
