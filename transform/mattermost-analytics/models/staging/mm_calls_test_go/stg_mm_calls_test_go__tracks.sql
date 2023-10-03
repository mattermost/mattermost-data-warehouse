SELECT
        server_version as server_version,
        plugin_build as plugin_build,
        event as event,
        user_id as server_id,
        received_at as received_at,
        plugin_version as plugin_version,
        event_text as event_text,
        id as event_id,
        timestamp as timestamp,
        actual_user_id as user_id
    FROM
        {{ ref ('base_mm_calls_test_go__tracks') }}
