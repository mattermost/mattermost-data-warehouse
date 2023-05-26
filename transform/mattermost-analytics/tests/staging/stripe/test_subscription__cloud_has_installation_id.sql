select
    s.subscription_id,
    p.name as plan_name
from
    {{ ref('stg_stripe__subscriptions')}} s
    left join {{ ref('stg_stripe__products')}} p on s.product_id = p.product_id
where
    -- Cloud subscription/subscription items
    p.name ilike '%cloud%'
    and s.cws_installation is null
