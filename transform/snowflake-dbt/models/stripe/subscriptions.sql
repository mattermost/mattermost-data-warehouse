{{config({
    "materialized": 'incremental',
    "schema": "stripe",
    "unique_key":"id",
    "tags":"hourly"
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
        ,subscriptions.trial_end
        ,subscriptions.trial_start
        ,subscriptions.quantity
        ,subscriptions.customer
        ,subscriptions.id
        ,subscriptions.livemode
        ,subscriptions.metadata:"cws-dns"::varchar as cws_dns
        ,subscriptions.metadata:"cws-blapi-subscription"::varchar as cws_blapi_subscription
        ,subscriptions.metadata:"cws-installation"::varchar as cws_installation
        ,subscriptions.metadata:"cws-installation-state"::varchar as cws_installation_state
        ,subscriptions.metadata:"billing-type"::varchar as billing_type
        ,subscriptions.metadata:"cws-renewed-from-stripe-id"::varchar as renewed_from_sub_id
        ,subscriptions.metadata:"cws-license-id"::varchar as license_id
        ,subscriptions.plan:"name"::varchar as edition
        ,subscriptions.metadata:"sfdc-migrated-opportunity-sfid"::varchar as sfdc_migrated_opportunity_sfid
        ,subscriptions.metadata:"internal_purchase_order"::varchar as purchase_order_num
        ,TO_TIMESTAMP_NTZ(subscriptions.metadata:"cws-date-converted-to-paid"::int) as date_converted_to_paid
        ,TO_TIMESTAMP_NTZ(subscriptions.metadata:"cws-license-end-date"::int) as license_end_date
        ,TO_TIMESTAMP_NTZ(subscriptions.metadata:"cws-actual-renewal-date"::int / 1000) as actual_renewal_date
        ,subscriptions."START"
        ,subscriptions.status
        ,subscriptions.updated
        ,subscriptions.plan
        ,subscriptions.metadata        
        ,subscriptions.ended_at
        ,subscriptions.canceled_at
        ,subscriptions.updated_by_event_type
    FROM {{ source('stripe_raw','subscriptions') }}
    {% if is_incremental() %}

    WHERE subscriptions.created::date >= (SELECT MAX(created::date) FROM {{ this }})
    OR subscriptions.updated::date >= (SELECT MAX(updated::date) FROM {{ this }})

    {% endif %}
), subscription_total AS (
    SELECT
        subscriptions.id,
        SUM(subscription_items.plan_amount * subscription_items.quantity) as total_in_cents
    FROM subscriptions
    JOIN {{ ref('subscription_items') }} on subscriptions.id = subscription_items.subscription
    GROUP BY 1
)
select subscriptions.*, coalesce(subscription_total.total_in_cents, 0) as total_in_cents
from subscriptions
left join subscription_total ON subscriptions.id = subscription_total.id
