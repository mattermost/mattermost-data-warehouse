{%- set include_columns = [ "user_id" ] -%}

WITH tracks AS (
    SELECT
        {{ dbt_utils.star(ref('base_mm_calls_test_go__tracks')) }}
    FROM
        {{ ref ('base_mm_calls_test_go__tracks') }}
)
SELECT
        participants as participants,
        server_version as server_version,
        call_id as call_id,
        plugin_build as plugin_build,
        event as event,
        user_id as user_id,
        received_at as received_at,
        plugin_version as plugin_version,
        duration as duration,
        event_text as event_text,
        id as event_id,
        timestamp as timestamp,
        screen_duration as screen_duration,
        actual_user_id as user_actual_id
FROM tracks