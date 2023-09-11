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

FROM tracks