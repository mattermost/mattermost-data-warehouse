-- Assert that only one renewal exists per subscription.
-- Renewals are identified by renewed_from_subscription_id pointing to the previous subscription.
-- Since each renewal creates a new subscription, each subscription should have 0 or 1 subscriptions
-- referring to it via renewed_from_subscription_id.
select
    s.renewed_from_subscription_id,
    count(s.subscription_id)
from
    {{ ref('stg_stripe__subscriptions')}} s
where
    s.renewed_from_subscription_id is not null
    -- Exceptions
    and s.subscription_id not in (
        -- Edge case: cancelled and refunded
        'sub_1L485eI67GP2qpb4UwghXXun'
    )
group by
    s.renewed_from_subscription_id
having
    count(s.subscription_id) > 1