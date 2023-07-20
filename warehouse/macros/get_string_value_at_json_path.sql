{% macro get_string_value_at_json_path(column, json_relative_path) %}
STRING(JSON_QUERY({{ column }}, '$.{{ json_relative_path }}'))
{% endmacro %}
