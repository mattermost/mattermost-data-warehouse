{%- set include_columns = ["plugin_version", "user_id", "actual_user_id", "message_type", "server_version"] -%}

WITH tracks AS (
    SELECT
        {{ get_rudderstack_columns() }}
        , {% for column in include_columns %}
        {{ column }} AS {{ column }}
        {% if not loop.last %},{% endif %}
    {% endfor %}
    FROM
        {{ source('mm_calls_test_go', 'tracks') }}
)
SELECT
    id                  AS event_id 
    , received_at       AS received_at
    , timestamp         AS timestamp
    , context_ip        AS context_ip
    , event             AS event_table
    , event_text        AS event_name
    , plugin_version    AS plugin_version
    , user_id           AS server_id
    , actual_user_id    AS user_id
    , message_type      AS message_type
    , server_version    AS server_version
FROM tracks