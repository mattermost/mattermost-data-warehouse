select
    id as event_id
    , event as event_table
    , event_text as event_name
    , user_id as server_id
    , coalesce(actual_user_id, participant_id) as user_id
    , received_at as received_at
    , timestamp  as timestamp
    , server_version as server_version
    , plugin_build as plugin_build
    , plugin_version as plugin_version
    -- Backfill past events
    , coalesce(context_feature_name, 'Calls') as feature_name
    , parse_json(coalesce(context_feature_skus, '[]'))::array as feature_skus
from
    {{ ref ('base_mm_calls_test_go__tracks') }}
