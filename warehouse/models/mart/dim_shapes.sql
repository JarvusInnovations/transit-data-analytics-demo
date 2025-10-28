{{ config(materialized='table') }}

with shapes as (
    select
        *,
        st_geogpoint(shape_pt_lon, shape_pt_lat) as shape_point
    from {{ ref('stg_shapes') }}
),

feeds as (
    select * from {{ ref('dim_schedule_feeds') }}
),

join_feeds as (
    select
        feeds._valid_from,
        feeds._valid_to,
        shapes._b64_url,
        shapes.shape_point,
        shapes.shape_pt_sequence,
        shapes.shape_id
    from feeds
    left join shapes
        on
            feeds._b64_url = shapes._b64_url
            and feeds._valid_from = shapes.dt
),

dim_shapes as (
    select
        _b64_url,
        shape_id,
        _valid_from,
        _valid_to,
        st_makeline(array_agg(shape_point order by shape_pt_sequence)) as shape_linestring
    from join_feeds
    group by _b64_url, shape_id, _valid_from, _valid_to
)

select * from dim_shapes
