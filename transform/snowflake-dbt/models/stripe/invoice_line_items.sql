{{config({
    "materialized": 'table',
    "schema": "stripe"
  })
}}

WITH invoice_line_items AS (
    SELECT 
        invoice_line_items.amount
        ,invoice_line_items.currency
        ,invoice_line_items.description
        ,invoice_line_items.discountable
        ,invoice_line_items.id
        ,invoice_line_items.invoice
        ,invoice_line_items.livemode
        ,invoice_line_items.period:"end"::datetime as period_end
        ,invoice_line_items.period:"start"::datetime as period_start
        ,invoice_line_items.plan:"active"::boolean as plan_active
        ,invoice_line_items.plan:"aggregate_usage"::varchar as plan_aggregate_usage
        ,invoice_line_items.plan:"amount"::varchar as plan_amount
        ,invoice_line_items.plan:"billing_scheme"::varchar as plan_billing_scheme
        ,invoice_line_items.plan:"created"::datetime as plan_created
        ,invoice_line_items.plan:"currency"::varchar as plan_currency
        ,invoice_line_items.plan:"id"::varchar as plan_id
        ,invoice_line_items.plan:"interval"::varchar as plan_interval
        ,invoice_line_items.plan:"interval_count"::varchar as plan_interval_count
        ,invoice_line_items.plan:"livemode"::boolean as plan_livemode
        ,invoice_line_items.plan:"nickname"::varchar as plan_nickname
        ,invoice_line_items.plan:"object"::varchar as plan_object
        ,invoice_line_items.plan:"product"::varchar as plan_product
        ,invoice_line_items.plan:"tiers"::varchar as plan_tiers
        ,invoice_line_items.plan:"tiers_mode"::varchar as plan_tiers_mode
        ,invoice_line_items.plan:"transform_usage"::varchar as plan_transform_usage
        ,invoice_line_items.plan:"trial_period_days"::integer as plan_trial_period_days
        ,invoice_line_items.plan:"usage_type"::varchar as plan_usage_type
        ,invoice_line_items.proration
        ,invoice_line_items.quantity
        ,invoice_line_items.subscription
        ,invoice_line_items.subscription_item
        ,invoice_line_items.type
        ,invoice_line_items.invoice_item
    FROM {{ source('stripe_raw','invoice_line_items') }}
)

select * from invoice_line_items