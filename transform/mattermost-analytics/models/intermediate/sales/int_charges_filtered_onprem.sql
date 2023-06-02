{{config({
    "materialized": 'table',
    "unique_key":"charge_id",
  })
}}

select
    s.subscription_id,
    s.customer_id,
    s.license_id,
    i.invoice_id,
    i.charge_id,
    s.start_at,
    s.ended_at,
    p.name as plan_name,
    s.license_start_at,
    s.license_end_at,
    s.actual_renewal_at,
    ili.quantity,
    ili.quantity - LAG(ili.quantity) OVER (PARTITION BY s.customer_id ORDER BY i.created_at) as seat_difference,
    datediff(day, actual_renewal_at, lag(license_end_at) OVER (PARTITION BY s.customer_id ORDER BY i.created_at)) as days_since_previous_license_end,
    row_number() over(partition by s.customer_id order by i.created_at) as charge_order,
    case when days_since_previous_license_end < 60 then true else false end as is_renewal,
    case when seat_difference > 0 then true else false end as is_expansion,
    case when seat_difference < 0 then true else false end as is_contraction
from
    {{ ref('stg_stripe__subscriptions')}} s
    left join {{ ref('stg_stripe__subscription_items')}} si on s.subscription_id = si.subscription_id
    left join {{ ref('stg_stripe__products')}} p on si.product_id = p.product_id
    left join {{ ref('stg_stripe__invoices')}} i on i.subscription_id = s.subscription_id
    left join {{ ref('stg_stripe__invoice_line_items')}} ili on ili.invoice_id = s.invoice_id
where
    -- Onprem subscription/subscription items
    p.name not ilike '%cloud%'
    -- Skip support subscription items and focus on main plan
    and p.name <> 'Premier Support'
    -- Skip incomplete subscriptions
    and s.status <> 'incomplete_expired'
    -- Ignoring subscriptions that come from admin portal/sales
    and s.renewal_type is null
    -- Data before this date might not be in-line with specification
    and s.created_at > '2021-04-01'
