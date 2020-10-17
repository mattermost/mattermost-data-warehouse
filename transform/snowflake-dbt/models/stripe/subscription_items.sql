{{config({
    "materialized": 'incremental',
    "schema": "stripe",
    "unique_key":"id",
    "tags":"hourly"
  })
}}

WITH subscription_items AS (
    SELECT 
        subscription_items.created
        ,subscription_items.id
        ,subscription_items.plan:"active"::boolean as active
        ,subscription_items.plan:"aggregate_usage"::varchar as plan_aggregate_usage
        ,subscription_items.plan:"amount"::integer as plan_amount
        ,subscription_items.plan:"billing_scheme"::varchar as plan_billing_scheme
        ,subscription_items.plan:"created"::datetime as plan_created
        ,subscription_items.plan:"currency"::varchar as plan_currency
        ,subscription_items.plan:"id"::varchar as plan_id
        ,subscription_items.plan:"interval"::varchar as plan_interval
        ,subscription_items.plan:"interval_count"::varchar as plan_interval_count
        ,subscription_items.plan:"livemode"::boolean as plan_livemode
        ,subscription_items.plan:"name"::varchar as plan_name
        ,subscription_items.plan:"nickname"::varchar as plan_nickname
        ,subscription_items.plan:"product"::varchar as plan_product
        ,subscription_items.plan:"statement_description"::varchar as plan_statement_description
        ,subscription_items.plan:"statement_descriptor"::varchar as plan_statement_descriptor
        ,subscription_items.plan:"tiers"::varchar as plan_tiers
        ,subscription_items.plan:"tiers_mode"::varchar as plan_tiers_mode
        ,subscription_items.plan:"transform_usage"::varchar as plan_transform_usage
        ,subscription_items.plan:"trial_period_days"::integer as plan_trial_period_days
        ,subscription_items.plan:"usage_type"::varchar as plan_usage_type
        ,subscription_items.quantity
        ,subscription_items.subscription
    FROM {{ source('stripe_raw','subscription_items') }}
    {% if is_incremental() %}

    WHERE subscription_items.created::date >= (SELECT MAX(created::DATE) FROM {{ this }} )

    {% endif %}
)

select * from subscription_items