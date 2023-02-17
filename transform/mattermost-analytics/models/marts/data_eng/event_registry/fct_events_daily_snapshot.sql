{{
    config({
        "tags":"hourly",
        "cluster_by": ['event_date'],
    })
}}

SELECT DISTINCT
    daily_event_id AS id
    , date_received_at AS event_date
    , event_id
    , event_count
FROM
    {{ ref('int_events_aggregated_to_date') }}
