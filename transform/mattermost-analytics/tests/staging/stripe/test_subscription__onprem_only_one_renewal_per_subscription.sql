-- Test that only one renewal exists per subscription
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
