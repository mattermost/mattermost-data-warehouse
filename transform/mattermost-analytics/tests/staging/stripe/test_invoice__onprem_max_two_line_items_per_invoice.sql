
with onprem_subscriptions as (
    select
        s.customer_id,
        s.subscription_id,
        s.renewed_from_subscription_id,
        p.name as plan_name,
        s.license_id,
        s.license_start_at,
        s.license_end_at,
        s.actual_renewal_at,
        s.sfdc_migrated_license_id is not null as is_sfdc_migrated,
        s.is_admin_generated_license,
        s.renewal_type, -- for debugging
        case when s.renewal_type = 'sales-only' then true else false end as is_only_renewed_from_sales
    from
        {{ ref('stg_stripe__subscriptions')}} s
        left join {{ ref('stg_stripe__subscription_items')}} si on s.subscription_id = si.subscription_id
        left join {{ ref('stg_stripe__products')}} p on si.product_id = p.product_id
    where
        -- Onprem subscription/subscription items
        p.name not ilike '%cloud%'
        -- Skip support subscription items and focus on main plan
        and p.name <> 'Premier Support'
        -- Skip incomplete subscriptions
        and s.status <> 'incomplete_expired'
        -- Data before this date might not be in-line with specification
        and s.created_at > '2021-04-01'
)
select
    i.invoice_id,
    count(ili.invoice_line_item_id) as total_invoice_line_items
from
    --- Join on onprem subscriptions in order to prune records
    onprem_subscriptions ds
    join {{ ref('stg_stripe__invoices')}} i on i.subscription_id = ds.subscription_id
    left join {{ ref('stg_stripe__invoice_line_items')}} ili on ili.invoice_id = i.invoice_id
group by i.invoice_id
having count(ili.invoice_line_item_id) > 2