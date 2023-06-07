{{config({
    "materialized": 'table',
    "unique_key":"charge_id",
  })
}}

with denormalized_subscriptions as (
    select
        s.customer_id,
        s.subscription_id,
        s.renewed_from_subscription_id,
        s.license_id,
        i.invoice_id,
        i.charge_id,
        s.start_at,
        s.ended_at,
        p.name as plan_name,
        s.license_start_at,
        s.license_end_at,
        s.actual_renewal_at,
        ili.amount,
        ili.quantity as number_of_seats,
        i.invoice_id is null as is_missing_invoice,
        s.sfdc_migrated_license_id is not null as is_sfdc_migrated,
        s.is_admin_generated_license,
        case when s.renewal_type = 'sales-only' then true else false end as is_only_renewed_from_sales
    from
        {{ ref('stg_stripe__subscriptions')}} s
        left join {{ ref('stg_stripe__subscription_items')}} si on s.subscription_id = si.subscription_id
        left join {{ ref('stg_stripe__products')}} p on si.product_id = p.product_id
        left join {{ ref('stg_stripe__invoices')}} i on i.subscription_id = s.subscription_id
        left join {{ ref('stg_stripe__invoice_line_items')}} ili on ili.invoice_id = i.invoice_id
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
    s.*,
    s.number_of_seats - parent.number_of_seats as seats_diff,
    datediff(day, parent.license_end_at, s.license_start_at) as days_since_previous_license_end,
    datediff(day, parent.actual_renewal_at, s.license_start_at) as days_since_actual_license_end,
    s.renewed_from_subscription_id is not null as is_renewal,
    seats_diff > 0 as is_expansion,
    seats_diff < 0 as is_contraction
from
    denormalized_subscriptions s
    left join denormalized_subscriptions parent on s.renewed_from_subscription_id = parent.subscription_id
where
    -- Exclude edge cases
    s.subscription_id not in (
        'sub_1L485eI67GP2qpb4UwghXXun'
    )
