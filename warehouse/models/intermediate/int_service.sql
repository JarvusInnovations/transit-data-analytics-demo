with cal as (
    select * from {{ ref('int_long_calendar') }}
),

cal_dates as (
    select
        {{ parse_schedule_filename('_FILE_NAME') }} as _b64_url, --noqa: CP02
        dt,
        JSON_VALUE(record, '$.service_id') as service_id,
        PARSE_DATE('%Y%m%d', JSON_VALUE(record, '$.date')) as service_date,
        case CAST(JSON_VALUE(record, '$.exception_type') as int)
            when 1 then true
            when 2 then false
        end as exception_type
    from {{ source('transit_data', 'gtfs_schedule__calendar_dates') }}
),

feeds as (
    select * from {{ ref('dim_schedule_feeds') }}
),

-- todo: make the start/end dates here more robust
dates as ({{ dbt_utils.date_spine(
    datepart="day",
    start_date="cast('2023-07-01' as date)",
    end_date="cast('2025-12-31' as date)"
   )
}}),

feed_dates as (
    select
        dates.*,
        feeds.*
    from dates
    left join feeds
        on dates.date_day between feeds._valid_from and feeds._valid_to
),

int_service as (
    select
        cal.* except (_b64_url, dt, service_id),
        cal_dates.* except (_b64_url, dt, service_id, service_date),
        feed_dates.date_day as service_date,
        COALESCE(cal.service_id, cal_dates.service_id) as service_id,
        COALESCE(cal._b64_url, cal_dates._b64_url) as _b64_url,
        COALESCE(cal.dt, cal_dates.dt) as dt,
        COALESCE(cal_dates.exception_type, cal.service_ind) as has_service
    from feed_dates --noqa: ST09
    left join cal
        on
            feed_dates.date_day between cal.start_date and cal.end_date
            and cal.service_ind
            and EXTRACT(dayofweek from feed_dates.date_day) = cal.day_of_week_int
            and feed_dates._b64_url = cal._b64_url
            and feed_dates._valid_from = cal.dt
    left join cal_dates
        on
            DATE(feed_dates.date_day) = cal_dates.service_date
            and (cal_dates.service_id = cal.service_id or cal.service_id is null)
            and feed_dates._b64_url = cal_dates._b64_url
            and feed_dates._valid_from = cal_dates.dt
)

select * from int_service
