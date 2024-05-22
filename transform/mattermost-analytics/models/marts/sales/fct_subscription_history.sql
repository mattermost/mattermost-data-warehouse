select 
    sh.subscription_history_event_id
    , sh.subscription_id
    , s.customer_id
    , COALESCE(sh.licensed_seats, s.quantity) as licensed_seats
    , sh.created_at
    , s.cws_dns
    , s.cws_installation
<<<<<<< HEAD
    , s.license_start_at
    , s.license_end_at
    , s.billing_type
    , s.status
=======
>>>>>>> master
    , row_number() over(partition by sh.subscription_id order by sh.created_at desc) = 1 as is_latest
from
    {{ ref('stg_cws__subscription_history') }} sh
    left join {{ ref('stg_stripe__subscriptions') }} s on sh.subscription_id = s.subscription_id