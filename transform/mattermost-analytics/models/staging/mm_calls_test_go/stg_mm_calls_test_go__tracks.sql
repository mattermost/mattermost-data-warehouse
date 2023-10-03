SELECT
     id AS event_id
    , event_text AS event_name
    , event AS event_table
    , actual_user_id AS user_id
    , user_id AS server_id
    , received_at AS received_at
    , timestamp  AS timestamp
    , server_version as server_version
    , plugin_build as plugin_build,
    , plugin_version as plugin_version,
    FROM
        {{ ref ('base_mm_calls_test_go__tracks') }}
