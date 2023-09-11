{%- set include_columns = [ "user_id" ] -%}

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
    , user_id           AS server_id
FROM tracks