-- Warn until the edge cases are clarified
{{ config(
    severity = 'warn',
    error_if = '> 2'
) }}

select
    s.subscription_id, count(s.subscription_item_id)
from
    {{ ref('stg_stripe__subscription_items')}} s
    left join {{ ref('stg_stripe__products')}} p on s.product_id = p.product_id
where
    p.name not ilike '%cloud%'
group by subscription_id
having count(s.subscription_item_id) > 2