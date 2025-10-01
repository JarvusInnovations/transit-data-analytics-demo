with cal as (
    select
        {{ parse_schedule_filename('_FILE_NAME') }} as _b64_url, --noqa: CP02
        dt,
        hour as hr,
        JSON_VALUE(record, '$.service_id') as service_id,
        JSON_VALUE(record, '$.monday') as monday, --noqa: RF04
        JSON_VALUE(record, '$.tuesday') as tuesday, --noqa: RF04
        JSON_VALUE(record, '$.wednesday') as wednesday, --noqa: RF04
        JSON_VALUE(record, '$.thursday') as thursday, --noqa: RF04
        JSON_VALUE(record, '$.friday') as friday, --noqa: RF04
        JSON_VALUE(record, '$.saturday') as saturday, --noqa: RF04
        JSON_VALUE(record, '$.sunday') as sunday, --noqa: RF04
        JSON_VALUE(record, '$.start_date') as start_date,
        JSON_VALUE(record, '$.end_date') as end_date
    from {{ source('transit_data', 'gtfs_schedule__calendar') }}
),

long_cal as (
    select *
    from cal
    unpivot (service_ind for day_of_week in (monday, tuesday, wednesday, thursday, friday, saturday, sunday))
),

int_long_calendar as (
    select
        service_id,
        day_of_week,
        _b64_url,
        dt,
        hr,
        PARSE_DATE('%Y%m%d', start_date) as start_date,
        PARSE_DATE('%Y%m%d', end_date) as end_date,
        -- https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#extract
        case day_of_week
            when 'monday' then 2
            when 'tuesday' then 3
            when 'wednesday' then 4
            when 'thursday' then 5
            when 'friday' then 6
            when 'saturday' then 7
            when 'sunday' then 1
        end as day_of_week_int,
        CAST(CAST(service_ind as int) as bool) as service_ind
    from long_cal
)

select * from int_long_calendar
