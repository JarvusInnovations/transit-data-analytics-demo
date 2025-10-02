{% macro gtfs_time_to_seconds(colname) %}
    CAST(SPLIT({{ colname }}, ':')[0] as int) * 3600
    + CAST(SPLIT({{ colname }}, ':')[1] as int) * 60
    + CAST(SPLIT({{ colname }}, ':')[2] as int)
{% endmacro %}
