{% macro metric_stop_performance_window_over() %}
    over(partition by stop_key, pick_label, hour_type)
{% endmacro %}

{% macro metric_stop_shape_performance_window_over() %}
    over(partition by stop_key, pick_label, hour_type, shape_id)
{% endmacro %}
