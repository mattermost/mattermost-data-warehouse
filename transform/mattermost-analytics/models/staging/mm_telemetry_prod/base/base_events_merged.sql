-- View containing the merge of base events and base events delta. This view is used to avoid re-deduping the same data.
-- Should be used as a replacement of source('mm_telemetry_prod', 'event') in downstream models.

{%
    set excludes = [
        'CONTEXT_TRAITS_ENVIRONMENT_HOST_TIER'
    ]
%}

select
    {{ dbt_utils.star(from=source('rudder_support', 'base_events'), except=excludes) }}
from
    {{ source('rudder_support', 'base_events') }} b
where
    not exists (select 1 from {{ ref('base_events_delta') }} d where d.id = b.id)

union all

-- Note that base_events_delta is already deduped. This helps avoid re-deduping the same data every time the view is
-- called.
select
    {{ dbt_utils.star(from=ref('base_events_delta'), except=excludes) }}
from
    {{ ref('base_events_delta') }}