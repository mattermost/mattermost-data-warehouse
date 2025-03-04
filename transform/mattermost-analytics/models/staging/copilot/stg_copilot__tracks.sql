select
    id as event_id
    , event as event_table
    , event_text as event_name
    , user_id as server_id
    , coalesce(actual_user_id, user_actual_id) as user_id
    , received_at as received_at
    , timestamp  as timestamp
    -- Backfill past events
    , coalesce(context_feature_name, 'Copilot') as feature_name
    , parse_json(coalesce(context_feature_skus, '[]'))::array as feature_skus
    -- Extras for group
    , plugin_version
    , plugin_build
    , server_version
    , bot_id
    , bot_service_type
from
    {{ ref ('base_copilot__tracks') }}
