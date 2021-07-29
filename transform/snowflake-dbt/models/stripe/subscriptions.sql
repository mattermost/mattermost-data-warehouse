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
        ,subscriptions.metadata:"billing-type"::varchar as billing_type
        ,subscriptions.metadata:"cws-renewed-from-sub"::varchar as renewed_from_sub_id
        ,subscriptions.metadata:"cws-license-id"::varchar as license_id
        ,subscriptions.metadata:"sfdc-migrated-opportunity-sfid"::varchar as sfdc_migrated_opportunity_sfid
        ,subscriptions.metadata:"internal_purchase_order"::varchar as purchase_order_num
        ,TO_TIMESTAMP_NTZ(subscriptions.metadata:"cws-license-end-date"::int) as license_end_date
        ,TO_TIMESTAMP_NTZ(subscriptions.metadata:"cws-actual-renewal-date"::int / 1000) as actual_renewal_date
        ,subscriptions."START"
        ,subscriptions.status
        ,subscriptions.updated
    FROM {{ source('stripe_raw','subscriptions') }}
    {% if is_incremental() %}

    WHERE subscriptions.created::date >= (SELECT MAX(created::date) FROM {{ this }})
    OR subscriptions.updated::date >= (SELECT MAX(updated::date) FROM {{ this }})

    {% endif %}
)

select * from subscriptions
