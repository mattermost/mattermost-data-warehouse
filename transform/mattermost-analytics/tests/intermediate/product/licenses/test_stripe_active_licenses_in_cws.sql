-- Checks that all stripe licenses exist in CWS.
-- It's possible that Stripe data are synchronized before CWS data, resulting in a temporary inconsistency.
-- Since syncs are hourly and the rate of license creation is not that fast, a small buffer is allowed before raising
-- a warning. This should help tackle the race condition.
{{ config(
    severity = 'warn',
    error_if = '> 3'
) }}

with onprem_licenses as (
    -- List of onprem licenses
    select
        s.license_id
    from
        {{ ref('stg_stripe__subscriptions')}} s
        left join {{ ref('stg_stripe__subscription_items')}} si on s.subscription_id = si.subscription_id
        left join {{ ref('stg_stripe__products')}} p on si.product_id = p.product_id
    where
        -- Onprem subscription/subscription items
        p.name not ilike '%cloud%'
        -- Skip support subscription items and focus on main plan
        and p.name <> 'Premier Support'
        -- Skip incomplete subscriptions
        and s.status <> 'incomplete_expired'
        -- Data before this date might not be in-line with specification
        and s.created_at > '2021-04-01'
        -- Ignore invalid records
        and s.license_id is not null
        -- License must be active
        and to_timestamp_ntz(date_part(epoch_second, license_end_at)) >= CURRENT_DATE

)
select
    op.license_id
from
    onprem_licenses op
    left join {{ ref('stg_cws__license')}} cws on op.license_id = cws.license_id
where
    cws.license_id is null