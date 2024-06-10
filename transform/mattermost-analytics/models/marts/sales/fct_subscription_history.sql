select 
    sh.subscription_history_event_id
    , s.subscription_id
    , s.customer_id
    , COALESCE(sh.licensed_seats, s.metadata:"license-seats", s.quantity) as licensed_seats
    , sh.created_at
    , s.cws_dns
    , s.cws_installation
    , s.license_start_at
    , s.license_end_at
    , s.billing_type
    , s.status
    , CASE 
        WHEN sh.subscription_id IS NULL THEN true -- If there isn't a matching subscription history value this is treated as the latest one
        ELSE ROW_NUMBER() OVER (PARTITION BY sh.subscription_id ORDER BY sh.created_at DESC) = 1 
    END AS is_latest
from
    {{ ref('stg_cws__subscription_history') }} sh
    right join {{ ref('stg_stripe__subscriptions') }} s on sh.subscription_id = s.subscription_id
    where s.cws_installation IS NOT NULL