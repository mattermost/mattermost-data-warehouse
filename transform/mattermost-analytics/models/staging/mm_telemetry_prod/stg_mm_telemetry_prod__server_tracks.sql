SELECT
     id               AS event_id
     , event          AS event_table
     , event_text     AS event_name
     , user_id        AS server_id
     , user_actual_id AS user_id
     , received_at    AS received_at
     , timestamp      AS timestamp
     , coalesce(context_useragent, context_user_agent) AS context_user_agent
    -- Backfill past events
    , coalesce(context_feature_name, 'unknown_features') as feature_name
    , parse_json(coalesce(context_feature_skus, '[]'))::array as feature_skus
FROM
    {{ ref ('base_mm_telemetry_prod__tracks') }}