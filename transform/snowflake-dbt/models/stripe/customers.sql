{{config({
    "materialized": 'incremental',
    "schema": "stripe",
    "unique_key":"id",
    "tags":"hourly"
  })
}}

WITH customers AS (
    SELECT 
        customers.account_balance
        ,customers.created
        ,customers.default_source
        ,customers.delinquent
        ,customers.email
        ,customers.name
        ,customers.id
        ,customers.invoice_prefix
        ,customers.livemode
        ,customers.metadata:"ContactFirstName"::varchar as contactfirstname
        ,customers.metadata:"ContactLastName"::varchar as contactlastname
        ,customers.metadata:"cws-customer"::varchar as cws_customer
        ,customers.sources
        ,customers.updated
        ,customers.cards
        ,customers.currency
        ,replace(customers.metadata:"cws-first-purchase-intent-wire-transfer",'/','') as cws_first_purchase_intent_wire_transfer
        ,replace(customers.metadata:"cws-renewal-self-intent-wire-transfer",'/','') as cws_renewal_self_intent_wire_transfer
        ,replace(customers.metadata:"cws-monthly-sub-intent-wire-transfer",'/','') as cws_monthly_sub_intent_wire_transfer
    FROM {{ source('stripe_raw','customers') }}
    {% if is_incremental() %}

    WHERE customers.created >= (SELECT MAX(created::date) FROM {{ this }} )

    {% endif %}
)

select * from customers