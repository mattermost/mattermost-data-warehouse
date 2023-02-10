{{
    config({
        "materialized": "table",
        "tags":"hourly",
        "schema": "event_registry",
        "cluster_by": ['source', 'event_table'],
    })
}}
SELECT
    {{ dbt_utils.surrogate_key(['event_table', 'source']) }} AS id
    , event_table
    , event_name
    , source
    , MIN(event_date) AS first_triggered_at
    , MAX(event_date) AS last_triggered_at
    , SUM(event_count) AS event_total_count
    , SUM(event_count) / (DATEDIFF(day, first_triggered_at, CURRENT_TIMESTAMP) + 1) AS event_daily_average
FROM
    {{ ref('daily_event_stats') }}
GROUP BY
    event_table,
    event_name,
    source