with src_trips as (
    select
        *,
        _FILE_NAME --noqa: CP02
    from {{ source('transit_data', 'gtfs_schedule__trips') }}
),

stg_trips as (
    select
        {{ extract_b64_url_from_filename('_FILE_NAME') }} as _b64_url, --noqa: CP02
        dt,
        hour as hr,
        JSON_VALUE(record, '$.route_id') as route_id,
        JSON_VALUE(record, '$.trip_id') as trip_id,
        JSON_VALUE(record, '$.service_id') as service_id,
        JSON_VALUE(record, '$.trip_headsign') as trip_headsign,
        JSON_VALUE(record, '$.trip_short_name') as trip_short_name,
        JSON_VALUE(record, '$.direction_id') as direction_id,
        JSON_VALUE(record, '$.block_id') as block_id,
        JSON_VALUE(record, '$.shape_id') as shape_id
    from src_trips
)

select * from stg_trips
