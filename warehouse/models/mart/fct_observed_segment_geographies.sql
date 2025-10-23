{{ config(materialized='table') }}

with segments as (
    select * from {{ ref('fct_observed_segments') }}
),

shapes as (
    select * from {{ ref('dim_shapes') }}
),

fct_observed_segment_geographies as (
    select --noqa: ST06
        segments.segment_key,
        segments.feed_key,
        segments.feed_name,
        segments.segment_id,
        segments.segment_name,
        segments.schedule_b64_url,
        segments.shape_id,
        segments.schedule_dt,
        st_linesubstring(shapes.shape_linestring, segments.shape_closest_point_to_stop_as_pct, segments.next_stop_pct) as segment_linestring
    from segments
    left join shapes
        on
            segments.schedule_b64_url = shapes._b64_url
            and segments.schedule_dt = shapes._valid_from
            and segments.shape_id = shapes.shape_id
    where segments.shape_closest_point_to_stop_as_pct < segments.next_stop_pct
)

select * from fct_observed_segment_geographies
