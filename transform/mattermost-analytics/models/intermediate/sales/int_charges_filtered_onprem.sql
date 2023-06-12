{{config({
    "materialized": 'table',
    "unique_key":"charge_id",
  })
}}

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
), flattened_onprem_invoices as (
    -- Keep only the invoices for the onprem subscriptions.
    -- Multiple invoices may exist for the same subscription. This is usually happening for expansions/contractions.
    select
        i.subscription_id,
        i.invoice_id,
        i.charge_id,
        i.amount_paid,
        -- Get number of seats for current invoice by ignoring line items referring previous invoice.
        -- Max is used here in order to return the single not-null item.
        max(
            case
                when ili.proration_details:"credited_items":"invoice_line_items" is not null then null
                else ili.quantity
            end
        ) as number_of_seats,
        -- Keep first, last and previous charge in group
        lag(i.charge_id) over(partition by i.subscription_id order by i.created_at asc) as previous_charge,
        last_value(i.charge_id) over(partition by i.subscription_id order by i.created_at asc) as last_subscription_charge,
        i.charge_id = last_subscription_charge as is_subscriptions_last_charge
    from
        --- Join on onprem subscriptions in order to prune records
        onprem_subscriptions ds
        join {{ ref('stg_stripe__invoices')}} i on i.subscription_id = ds.subscription_id
        left join {{ ref('stg_stripe__invoice_line_items')}} ili on ili.invoice_id = i.invoice_id
    group by
        i.subscription_id,
        i.invoice_id,
        i.charge_id,
        i.amount_paid
), onprem_charges as (
    select
        os.*,
        foi.invoice_id,
        foi.charge_id,
        foi.amount_paid,
        foi.number_of_seats,
        -- Handle cases where previous charge is from a previous subscription
        coalesce(foi.previous_charge, lpc.charge_id) as previous_charge
    from
        onprem_subscriptions os
        left join flattened_onprem_invoices foi on os.subscription_id = foi.subscription_id
        left join flattened_onprem_invoices lpc on os.renewed_from_subscription_id = lpc.subscription_id and lpc.is_subscriptions_last_charge
)
select
    oc.*,
    oc.renewed_from_subscription_id is null as is_new_purchase,
    oc.renewed_from_subscription_id is not null as is_renewal,
    oc.number_of_seats - pc.number_of_seats as seats_diff,
    seats_diff > 0 as is_expansion,
    seats_diff < 0 as is_contraction
from
    onprem_charges oc
    left join onprem_charges as pc on oc.previous_charge_id = pc.charge_id
