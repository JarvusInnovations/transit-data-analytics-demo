with src_stops as (
    select
        *,
        _FILE_NAME --noqa: CP02
    from {{ source('transit_data', 'gtfs_schedule__stops') }}
),

stg_stops as (
    select
        {{ extract_b64_url_from_filename('_FILE_NAME') }} as _b64_url, --noqa: CP02
        dt,
        hour as hr,
        JSON_VALUE(record, '$.stop_id') as stop_id,
        CAST(JSON_VALUE(record, '$.stop_lat') as numeric) as stop_lat,
        CAST(JSON_VALUE(record, '$.stop_lon') as numeric) as stop_lon
        -- todo: add additional fields
    from src_stops
)

select * from stg_stops
