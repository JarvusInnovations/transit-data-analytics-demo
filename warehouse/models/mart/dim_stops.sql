{{ config(materialized='table') }}

with stops as (
    select
        *,
        st_geogpoint(stop_lon, stop_lat) as stop_pt
    from {{ ref('stg_stops') }}
),

feeds as (
    select * from {{ ref('dim_schedule_feeds') }}
),

join_feeds as (
    select
        feeds._valid_from,
        feeds._valid_to,
        stops._b64_url,
        stops.dt,
        stops.stop_pt,
        stops.stop_name,
        stops.stop_id
    from feeds
    left join stops
        on
            feeds._b64_url = stops._b64_url
            and feeds._valid_from = stops.dt
),

dim_stops as (
    select --noqa: ST06
        _b64_url,
        stop_id,
        {{ dbt_utils.generate_surrogate_key(['_b64_url', 'dt', 'stop_id']) }} as stop_key,
        _valid_from,
        _valid_to,
        stop_pt,
        stop_name
    from join_feeds
)

select * from dim_stops
