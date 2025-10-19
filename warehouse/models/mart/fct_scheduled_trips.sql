{{ config(materialized='table') }}

{% set dates = ['2023-10-09', '2023-10-10', '2023-10-11', '2023-10-12', '2023-10-13', '2023-10-14', '2023-10-15',
            '2024-10-07', '2024-10-08', '2024-10-09', '2024-10-10', '2024-10-11', '2024-10-12', '2024-10-13'] %}

with routes as (
    select * from {{ ref('stg_routes') }}
),

trips as (
    select * from {{ ref('stg_trips') }}
),

srvc as (
    select * from {{ ref('int_service') }}
),

fct_scheduled_trips as (
    select
        srvc.service_date,
        trips.trip_id,
        trips.route_id,
        trips.service_id,
        routes.route_type,
        routes.route_short_name,
        routes.route_long_name,
        trips._b64_url,
        trips.dt,
        trips.shape_id,
        srvc.feed_name
    from srvc
    left join trips
        on
            srvc._b64_url = trips._b64_url
            and srvc.dt = trips.dt
            and srvc.service_id = trips.service_id
    left join routes
        on
            trips._b64_url = routes._b64_url
            and trips.dt = routes.dt
            and trips.route_id = routes.route_id
    -- todo: remove this filter and deal with performance
    where srvc.service_date in (
        {% for date in dates %}
            '{{ date }}'
            {% if not loop.last %}, {% endif %}
        {% endfor %}
    )
)

select * from fct_scheduled_trips
