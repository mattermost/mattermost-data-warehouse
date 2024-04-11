
WITH events AS (
    SELECT
        *
    FROM
        {{ source('mattermost2', 'event') }}
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
     , CASE WHEN LOWER(context_user_agent) LIKE '%electron%' THEN 'desktop' ELSE 'web_app' END AS client_type
FROM events