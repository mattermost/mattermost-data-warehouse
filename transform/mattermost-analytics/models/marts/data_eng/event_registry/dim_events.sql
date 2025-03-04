select distinct
    event_id
    , event_name
    , event_table
    , category
    , event_type
    , source
from
    {{ ref('int_events_aggregated_to_date') }}
