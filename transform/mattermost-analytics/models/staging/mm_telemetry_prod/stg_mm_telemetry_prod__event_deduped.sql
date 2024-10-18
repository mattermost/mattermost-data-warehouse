select
    {{ dbt_utils.star(from=source('mm_telemetry_prod', 'event')) }} b
from
    {{ source('mm_telemetry_prod', 'event') }}
where
    not exists (select 1 from {{ ref('base_events_delta') }} d where d.id = b.id)

union all

-- Note that base_events_delta is already deduped. This helps avoid re-deduping the same data every time the view is
-- called.
select
    {{ dbt_utils.star(from=ref('base_events_delta')) }}
from
    {{ ref('base_events_delta') }}