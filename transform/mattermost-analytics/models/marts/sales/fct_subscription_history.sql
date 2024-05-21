select 
    sh.subscription_history_event_id
    , sh.subscription_id
    , s.customer_id
    , COALESCE(sh.licensed_seats, s.quantity) as licensed_seats
    , sh.created_at
    , s.cws_dns
    , s.cws_installation
    , row_number() over(partition by sh.subscription_id order by sh.created_at desc) = 1 as is_latest
from
    dbt_staging.stg_cws__subscription_history sh
    left join dbt_staging.stg_stripe__subscriptions s on sh.subscription_id = s.subscription_id