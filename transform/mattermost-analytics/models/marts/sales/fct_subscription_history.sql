select
    sh.subscription_history_event_id
    , s.subscription_id
    , s.customer_id
    , COALESCE(sh.licensed_seats, s.quantity) as licensed_seats
    , sh.created_at
    , s.created_at as subscription_created_at
    , s.converted_to_paid_at
    , s.cws_dns
    , s.cws_installation
    , s.license_start_at
    , s.license_end_at
    , s.billing_type
    , s.status
    , s.product_id
    , s.current_product_id
    , CASE 
        WHEN sh.subscription_id IS NULL THEN true -- If there isn't a matching subscription history value this is treated as the latest one
        ELSE ROW_NUMBER() OVER (PARTITION BY sh.subscription_id ORDER BY sh.created_at DESC) = 1 
    END AS is_latest
    , p.name as product_name
from
    {{ ref('stg_cws__subscription_history') }} sh
    right join {{ ref('stg_stripe__subscriptions') }} s on sh.subscription_id = s.subscription_id
    left join {{ ref('stg_stripe__products')}} p on coalesce(s.product_id, s.current_product_id) = p.product_id
    where s.cws_installation IS NOT NULL