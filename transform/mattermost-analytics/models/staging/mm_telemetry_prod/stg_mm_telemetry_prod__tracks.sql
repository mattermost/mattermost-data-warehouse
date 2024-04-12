
WITH tracks AS (
    SELECT
        {{ dbt_utils.star(ref('base_mm_telemetry_prod__tracks')) }}
    FROM
        {{ ref ('base_mm_telemetry_prod__tracks') }}
)
SELECT
     id               AS event_id
     , event          AS event_table
     , event_text     AS event_name
     , category       AS category
     , type           AS event_type
     , user_id        AS server_id
     , user_actual_id AS user_id
     , received_at    AS received_at
     , timestamp      AS timestamp
     , coalesce(context_useragent, context_user_agent) AS context_user_agent
FROM tracks