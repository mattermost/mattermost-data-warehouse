WITH performance_events AS (
    SELECT
        {{ get_rudderstack_columns() }}
        , {% for column in var('performance_metrics_properties') %}
            {{ column }} AS {{ column }}
            {% if not loop.last %},{% endif %}
        {% endfor %}
        , coalesce(context_useragent, context_user_agent) as context_user_agent
        , timestamp::date as event_date
        , received_at::date as received_at_date
    FROM
      {{ source('mm_telemetry_prod', 'event') }}
    WHERE CATEGORY = 'performance'
)
SELECT * FROM performance_events
