-------------- ALL --------------

{% macro read_file_config_and_partitions() %}

        dt,
        hour,
        JSON_VALUE(file,'$.config.url') AS _url,
        JSON_VALUE(file, '$.config.name') AS _name,
        TIMESTAMP(JSON_VALUE(file, '$.ts')) AS _extract_ts

{% endmacro %}

{% macro metadata_columns() %}
    dt,
    hour,
    _url,
    _name,
    _extract_ts
{% endmacro %}

-------------- GTFS RT --------------

-- given the path to the `trip` element (something like `entity.vehicle`, does not include initial `$.`)
-- parse a trip descriptor from JSONL
{% macro read_gtfs_rt_trip_descriptor(parent_json_path) %}

        JSON_VALUE(record, '$.{{ parent_json_path }}.trip.tripId') AS trip_id,
        JSON_VALUE(record, '$.{{ parent_json_path }}.trip.routeId') AS trip_route_id,
        JSON_VALUE(record, '$.{{ parent_json_path }}.trip.directionId') AS trip_direction_id,
        JSON_VALUE(record, '$.{{ parent_json_path }}.trip.startTime') AS trip_start_time,
        JSON_VALUE(record, '$.{{ parent_json_path }}.trip.startDate') AS trip_start_date,
        JSON_VALUE(record, '$.{{ parent_json_path }}.trip.scheduleRelationship') AS trip_schedule_relationship

{% endmacro %}

{% macro trip_descriptor_columns() %}
    trip_id,
    trip_route_id,
    trip_direction_id,
    trip_start_date,
    trip_start_time,
    trip_schedule_relationship
{% endmacro %}
