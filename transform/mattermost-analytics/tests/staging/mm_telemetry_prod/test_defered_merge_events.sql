{{
    config({
        'tags': ['hourly'],
    })
}}

-- Assert that there's no overlap between base and delta table.
-- Overlap should be addressed by the post-hook of the delta table.
with base_table as (
    select max(received_at) as last_received_at from {{ source('rudder_support', 'base_events') }}
), delta_table as (
    select min(received_at) as first_received_at from {{ ref('base_events_delta') }}
)
select
    base_table.last_received_at,
    delta_table.first_received_at
from
    base_table cross join delta_table
where
    dateadd(day, -2, base_table.last_received_at) > delta_table.first_received_at
