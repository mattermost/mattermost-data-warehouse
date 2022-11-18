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
        ,customers.metadata:"contactfirstname"::varchar as contactfirstname
        ,customers.metadata:"contactlastname"::varchar as contactlastname
        ,customers.metadata:"cws-customer"::varchar as cws_customer
        ,customers.sources
        ,customers.updated
        ,customers.cards
        ,customers.currency
        ,customers.metadata
    FROM {{ source('stripe_raw','customers') }}
    {% if is_incremental() %}

    WHERE customers.created::date >= (SELECT MAX(created::date) FROM {{ this }} )

    {% endif %}
)

select * from customers