-- View containing the merge of base events and base events delta. This view is used to avoid re-deduping the same data.
-- Should be used as a replacement of source('mm_telemetry_prod', 'event') in downstream models.

{%-
    set excludes = [
        'COLUMN_NAME',
        'CHANNEL_LEN',
        'SECOND_RECOMPUTATIONS',
        'DURATION',
        'SEATS',
        'CONTEXT_SCREEN_HEIGHT',
        'SECOND_EFFECTIVENESS',
        'USERS',
        'CONTEXT_TRAITS_ITEM_IN_CART',
        'CONTEXT_TRAITS_ORG_ID',
        'CONTEXT_RELEVANCE',
        'MAX_API_RESOURCE_SIZE',
        'REMAINING',
        'CONTEXT_SCREEN_DENSITY',
        'CONTEXT_SCREEN_INNER_WIDTH',
        'CONTEXT_TRAITS_ENVIRONMENT_HOST_TIER',
        'CONTEXT_SESSION_ID',
        'CONTEXT_TRAITS_ID',
        'TOTAL_DURATION',
        'FIRST_EFFECTIVENESS',
        'NUM_INVITATIONS',
        'NUM_MEDIUM',
        'CONTEXT_SCREEN_INNER_HEIGHT',
        'CONTEXT_SCREEN_WIDTH',
        'THIRD_RECOMPUTATIONS',
        'COUNT',
        'THIRD_EFFECTIVENESS',
        'REQUEST_COUNT',
        'NUM_HIGH',
        'CONTEXT_TERMINATORS_LASTINDEX',
        'CONTEXT_TRAITS_USER_ID',
        'NUM_LOW',
        'CONTEXT_TRAITS_NB_OF_AUDIENCE_VISIT',
        'CONTEXT_SCREGN_DENSITY',
        'NUM_TOTAL',
        'CONTEXT_TRAITS_ACTIVE_ADDRESS_ID',
        'FIRST_RECOMPUTATIONS',
        'TOTAL_SIZE',
        'NUM_OF_REQUEST',
        'INVITE_COUNT'
    ]
-%}


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