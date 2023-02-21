SELECT DISTINCT
    event_id
    , event_name
    , event_table
    , category
    , event_type
    , source
FROM
    {{ ref('int_events_aggregated_to_date') }}
