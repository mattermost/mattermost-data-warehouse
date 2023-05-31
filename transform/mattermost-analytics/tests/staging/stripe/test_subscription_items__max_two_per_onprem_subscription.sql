select
    si.subscription_id, count(si.subscription_item_id)
from
    {{ ref('stg_stripe__subscription_items')}} si
    left join {{ ref('stg_stripe__products')}} p on si.product_id = p.product_id
where
    p.name not ilike '%cloud%'
group by subscription_id
having count(si.subscription_item_id) > 2