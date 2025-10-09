with src_shapes as (
    select
        *,
        _FILE_NAME --noqa: CP02
    from {{ source('transit_data', 'gtfs_schedule__shapes') }}
),

stg_shapes as (
    select
        {{ extract_b64_url_from_filename('_FILE_NAME') }} as _b64_url, --noqa: CP02
        dt,
        hour as hr,
        json_value(record, '$.shape_id') as shape_id,
        cast(json_value(record, '$.shape_pt_lat') as numeric) as shape_pt_lat,
        cast(json_value(record, '$.shape_pt_lon') as numeric) as shape_pt_lon,
        cast(json_value(record, '$.shape_pt_sequence') as numeric) as shape_pt_sequence,
        cast(json_value(record, '$.shape_dist_traveled') as numeric) as shape_dist_traveled
    from src_shapes
)

select * from stg_shapes
