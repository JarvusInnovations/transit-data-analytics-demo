{% macro extract_b64_url_from_filename(colname) %}
    LEFT(SPLIT({{ colname }}, '/')[6],LENGTH(SPLIT({{ colname }}, '/')[6]) - 9)
{% endmacro %}
