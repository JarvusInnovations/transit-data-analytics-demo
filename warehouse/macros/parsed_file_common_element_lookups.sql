-------------- ALL --------------

{% macro read_file_config_and_partitions() %}

        dt,
        hour,
        {{ get_string_value_at_json_path('file' ,'config.url') }} AS _url,
        {{ get_string_value_at_json_path('file' , 'config.name') }} AS _name,
        TIMESTAMP({{ get_string_value_at_json_path('file', 'ts') }}) AS _extract_ts

{% endmacro %}

{% macro metadata_columns() %}
    dt,
    hour,
    _url,
    _name,
    _extract_ts
{% endmacro %}

-------------- GTFS RT --------------

-- given the path to the `trip` element (something like `{{ parent_json_path }}`, does not include initial `$.`)
-- parse a trip descriptor from JSONL
{% macro read_gtfs_rt_trip_descriptor(parent_json_path) %}

        {{ get_string_value_at_json_path('record', parent_json_path + '.trip.tripId') }} AS trip_id,
        {{ get_string_value_at_json_path('record', parent_json_path + '.trip.routeId') }} AS trip_route_id,
        {{ get_string_value_at_json_path('record', parent_json_path + '.trip.direectionId') }} AS trip_direction_id,
        {{ get_string_value_at_json_path('record', parent_json_path + '.trip.startTime') }} AS trip_start_time,
        {{ get_string_value_at_json_path('record', parent_json_path + '.trip.startDate') }} AS trip_start_date,
        {{ get_string_value_at_json_path('record', parent_json_path + '.trip.scheduleRelationship') }} AS trip_schedule_relationship

{% endmacro %}

{% macro trip_descriptor_columns() %}
    trip_id,
    trip_route_id,
    trip_direction_id,
    trip_start_date,
    trip_start_time,
    trip_schedule_relationship
{% endmacro %}
