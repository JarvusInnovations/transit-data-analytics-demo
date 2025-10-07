-------------- ALL --------------

-- this is broken: https://github.com/JarvusInnovations/transit-data-analytics-demo/issues/33
{% macro read_file_config_and_partitions() %}

        dt,
        hour,
        JSON_VALUE(file,'$.config.url') AS _url,
        JSON_VALUE(file, '$.config.name') AS _name,
        TIMESTAMP(JSON_VALUE(file, '$.ts')) AS _extract_ts

{% endmacro %}

-- workaround while the above more robust macro is broken
{% macro impute_parsed_rt_file_info() %}

        dt,
        hour,
        {{ extract_b64_url_from_filename('_FILE_NAME') }} as _b64_url

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

{% macro read_gtfs_rt_header(parent_json_path='header') %}

        JSON_VALUE(record, '$.{{ parent_json_path }}.gtfsRealtimeVersion') AS gtfs_realtime_version,
        JSON_VALUE(record, '$.{{ parent_json_path }}.incrementality') AS incrementality,
        JSON_VALUE(record, '$.{{ parent_json_path }}.timestamp') AS gtfs_rt_message_timestamp

{% endmacro %}

{% macro gtfs_rt_header_columns() %}
    gtfs_realtime_version,
    incrementality,
    gtfs_rt_message_timestamp
{% endmacro %}

-- given the path to the `vehicle` element (something like `entity.vehicle`, does not include initial `$.`)
-- parse a trip descriptor from JSONL
{% macro read_gtfs_rt_vehicle_descriptor(parent_json_path) %}

        JSON_VALUE(record, '$.{{ parent_json_path }}.vehicle.id') AS vehicle_id,
        JSON_VALUE(record, '$.{{ parent_json_path }}.vehicle.label') AS vehicle_label,
        JSON_VALUE(record, '$.{{ parent_json_path }}.vehicle.licensePlate') AS license_plate,
        JSON_VALUE(record, '$.{{ parent_json_path }}.vehicle.wheelchairAccessible') AS vehicle_wheelchair_accessible

{% endmacro %}

{% macro vehicle_descriptor_columns() %}
    vehicle_id,
    vehicle_label,
    license_plate,
    vehicle_wheelchair_accessible
{% endmacro %}
