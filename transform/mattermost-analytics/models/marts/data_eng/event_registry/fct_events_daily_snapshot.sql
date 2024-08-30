{{
    config({
        "cluster_by": ['event_date']
    })
}}

select distinct
    daily_event_id as id
    , received_at_date as event_date
    , event_id
    , event_count
from
    {{ ref('int_events_aggregated_to_date') }}
