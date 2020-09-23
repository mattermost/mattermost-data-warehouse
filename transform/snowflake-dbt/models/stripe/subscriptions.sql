{{config({
    "materialized": 'table',
    "schema": "stripe"
  })
}}

WITH subscriptions AS (
    SELECT 
        subscriptions.billing
        ,subscriptions.billing_cycle_anchor
        ,subscriptions.cancel_at_period_end
        ,subscriptions.created
        ,subscriptions.current_period_end
        ,subscriptions.current_period_start
        ,subscriptions.quantity
        ,subscriptions.customer
        ,subscriptions.id
        ,subscriptions.livemode
        ,subscriptions.metadata:"cws-dns"::varchar as cws_dns
        ,subscriptions.metadata:"cws-blapi-subscription"::varchar as cws_blapi_subscription
        ,subscriptions.metadata:"cws-installation"::varchar as cws_installation
        ,subscriptions."START"
        ,subscriptions.status
        ,subscriptions.updated
    FROM {{ source('stripe_raw','subscriptions') }}
)

select * from subscriptions
