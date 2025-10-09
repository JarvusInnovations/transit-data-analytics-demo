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
)

select * from fct_scheduled_trips
