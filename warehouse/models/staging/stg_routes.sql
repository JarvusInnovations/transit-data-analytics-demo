with src_routes as (
    select
        *,
        _FILE_NAME --noqa: CP02
    from {{ source('transit_data', 'gtfs_schedule__routes') }}
),

stg_routes as (
    select
        {{ extract_b64_url_from_filename('_FILE_NAME') }} as _b64_url, --noqa: CP02
        dt,
        hour as hr,
        JSON_VALUE(record, '$.route_id') as route_id,
        JSON_VALUE(record, '$.agency_id') as agency_id,
        JSON_VALUE(record, '$.route_short_name') as route_short_name,
        JSON_VALUE(record, '$.route_long_name') as route_long_name,
        JSON_VALUE(record, '$.route_desc') as route_desc,
        JSON_VALUE(record, '$.route_type') as route_type
        -- todo: unpack additional attributes
    from src_routes
)

select * from stg_routes
