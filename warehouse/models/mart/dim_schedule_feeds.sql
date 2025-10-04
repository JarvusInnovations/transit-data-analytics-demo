with schedule_outcomes as (
    select
        dt,
        PARSE_DATETIME('%Y-%m-%dT%H:%M:%SZ', JSON_VALUE(file, '$.ts')) as ts,
        JSON_VALUE(file, '$.config.name') as feed_name,
        JSON_VALUE(file, '$.config.url') as feed_url,
        JSON_VALUE(metadata, '$.hash') as feed_content_hash
    from {{ source('transit_data', 'parse_outcomes') }}
    where feed_type = 'gtfs_schedule'
),

feed_appearance_dates as (
    select
        feed_content_hash,
        feed_url,
        feed_name,
        -- this is not very robust -- e.g., if same feed appears once, removed, reappears
        -- also doesn't account for feed_info validity period
        MIN(ts) as _valid_from,
        MAX(ts) as max_ts
    from schedule_outcomes
    group by feed_content_hash, feed_url, feed_name
),

dim_schedule_feeds as (
    select
        feed_content_hash,
        feed_url,
        feed_name,
        -- this is not very robust -- e.g., if same feed appears once, removed, reappears
        -- also doesn't account for feed_info validity period
        _valid_from,
        COALESCE(LEAD(_valid_from) over (partition by feed_url order by _valid_from), max_ts) as _valid_to,
        TO_BASE64(CAST(feed_url as BYTES)) as _b64_url
    from feed_appearance_dates
)

select * from dim_schedule_feeds
