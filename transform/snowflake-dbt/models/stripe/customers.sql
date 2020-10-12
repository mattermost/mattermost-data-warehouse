{{config({
    "materialized": 'table',
    "schema": "stripe"
  })
}}

WITH customers AS (
    SELECT 
        customers.account_balance
        ,customers.created
        ,customers.default_source
        ,customers.delinquent
        ,customers.email
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
    FROM {{ source('stripe_raw','customers') }}
)

select * from customers