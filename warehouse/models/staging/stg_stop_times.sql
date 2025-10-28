with src_stop_times as (
    select
        *,
        _FILE_NAME --noqa: CP02
    from {{ source('transit_data', 'gtfs_schedule__stop_times') }}
),

stg_stop_times as (
    select
        {{ extract_b64_url_from_filename('_FILE_NAME') }} as _b64_url, --noqa: CP02
        dt,
        hour as hr,
        JSON_VALUE(record, '$.trip_id') as trip_id,
        JSON_VALUE(record, '$.stop_id') as stop_id,
        JSON_VALUE(record, '$.arrival_time') as arrival_time,
        JSON_VALUE(record, '$.departure_time') as departure_time,
        JSON_VALUE(record, '$.stop_sequence') as stop_sequence,
        JSON_VALUE(record, '$.timepoint') as timepoint
        -- todo: add additional fields
    from src_stop_times
)

select * from stg_stop_times
