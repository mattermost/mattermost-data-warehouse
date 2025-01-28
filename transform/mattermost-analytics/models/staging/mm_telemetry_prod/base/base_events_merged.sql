-- View containing the merge of base events and base events delta. This view is used to avoid re-deduping the same data.
-- Should be used as a replacement of source('mm_telemetry_prod', 'event') in downstream models.

{%
    set excludes = [

        'CONTEXT_SESSION_ID',
        'CONTEXT_TERMINATORS_LASTINDEX',
        'CONTEXT_TRAITS_ACTIVE_ADDRESS_ID',
        'CONTEXT_TRAITS_ENVIRONMENT_HOST_TIER',
        'CONTEXT_TRAITS_ID',
        'CONTEXT_TRAITS_ITEM_IN_CART',
        'CONTEXT_TRAITS_NB_OF_AUDIENCE_VISIT',
        'CONTEXT_TRAITS_ORG_ID',
        'CONTEXT_TRAITS_USER_ID'
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