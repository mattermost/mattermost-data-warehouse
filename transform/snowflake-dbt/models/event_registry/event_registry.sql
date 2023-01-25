{{
    config({
        "materialized": "table",
        "tags":"hourly",
        "schema": "event_registry",
        "cluster_by": ['source', 'event'],
    })
}}
SELECT
    {{ dbt_utils.surrogate_key(['event', 'source']) }} AS id
    , event
    , event_text
    , source
    , MIN(date_received_at) AS first_triggered_at
    , MAX(date_received_at) AS last_triggered_at
    , SUM(event_count) AS event_total_count
    , SUM(event_count) / DATEDIFF(day, first_triggered_at, CURRENT_TIMESTAMP) AS event_daily_average
FROM
    {{ ref('daily_event_stats') }}
GROUP BY
    event,
    event_text,
    source