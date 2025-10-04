{% macro parse_schedule_filename(colname) %}
    LEFT(SPLIT({{ colname }}, '/')[6],LENGTH(SPLIT({{ colname }}, '/')[6]) - 9)
{% endmacro %}
