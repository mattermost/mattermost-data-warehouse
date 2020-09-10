{{config({
    "materialized": 'table',
    "schema": "stripe"
  })
}}

WITH stripe_subscriptions AS (
    SELECT 
        subscriptions.billing
        ,subscriptions.billing_cycle_anchor
        ,subscriptions.cancel_at_period_end
        ,subscriptions.created
        ,subscriptions.current_period_end
        ,subscriptions.current_period_start
        ,subscriptions.customer
        ,subscriptions.id
        ,subscriptions.livemode
        ,subscriptions.metadata:"cws-dns"::varchar as cws_dns
        ,subscriptions.metadata:"cws-blapi-subscription"::varchar as cws_blapi_subscription
        ,subscriptions.metadata:"cws-previous-payment"::varchar as cws_prev_payment
        ,subscriptions.plan:"active"::boolean as active
        ,subscriptions.plan:"aggregate_usage"::varchar as plan_aggregate_usage
        ,subscriptions.plan:"amount"::integer as plan_amount
        ,subscriptions.plan:"billing_scheme"::varchar as plan_billing_scheme
        ,subscriptions.plan:"created"::datetime as plan_created
        ,subscriptions.plan:"currency"::varchar as plan_currency
        ,subscriptions.plan:"id"::varchar as plan_id
        ,subscriptions.plan:"interval"::varchar as plan_interval
        ,subscriptions.plan:"interval_count"::varchar as plan_interval_count
        ,subscriptions.plan:"livemode"::boolean as plan_livemode
        ,subscriptions.plan:"name"::varchar as plan_name
        ,subscriptions.plan:"nickname"::varchar as plan_nickname
        ,subscriptions.plan:"object"::varchar as plan_object
        ,subscriptions.plan:"product"::varchar as plan_product
        ,subscriptions.plan:"statement_description"::varchar as plan_statement_description
        ,subscriptions.plan:"statement_descriptor"::varchar as plan_statement_descriptor
        ,subscriptions.plan:"tiers"::varchar as plan_tiers
        ,subscriptions.plan:"tiers_mode"::varchar as plan_tiers_mode
        ,subscriptions.plan:"transform_usage"::varchar as plan_transform_usage
        ,subscriptions.plan:"trial_period_days"::integer as plan_trial_period_days
        ,subscriptions.plan:"usage_type"::varchar as plan_usage_type
        ,subscriptions.quantity
        ,subscriptions."START"
        ,subscriptions.status
        ,subscriptions.updated
    FROM {{ source('stripe','subscriptions') }}
)

select * from stripe_subscriptions
