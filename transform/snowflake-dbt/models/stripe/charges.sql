{{config({
    "materialized": 'table',
    "schema": "stripe"
  })
}}

WITH charges AS (
    SELECT 
        charges.amount
        ,charges.amount_refunded
        ,charges.balance_transaction
        ,charges.captured
        ,charges.created
        ,charges.currency
        ,charges.customer
        ,charges.failure_code
        ,charges.failure_message
        ,charges.fraud_details
        ,charges.id
        ,charges.livemode
        ,charges.outcome:"network_status"::varchar as network_status
        ,charges.outcome:"reason"::varchar as reason
        ,charges.outcome:"risk_level"::varchar as risk_level
        ,charges.outcome:"seller_message"::varchar as seller_message
        ,charges.outcome:"type"::varchar as type
        ,charges.paid
        ,charges.payment_intent
        ,charges.payment_method_details:"card":"brand"::varchar as payment_method_card_brand
        ,charges.payment_method_details:"card":"checks":"address_line1_check"::varchar as payment_method_card_checks_address_line1
        ,charges.payment_method_details:"card":"checks":"address_postal_code_check"::varchar as payment_method_card_checks_postal_code
        ,charges.payment_method_details:"card":"checks":"cvc_check"::varchar as payment_method_card_checks_cvc
        ,charges.payment_method_details:"card":"country"::varchar as payment_method_card_country
        ,charges.payment_method_details:"card":"exp_month"::varchar as payment_method_card_exp_month
        ,charges.payment_method_details:"card":"exp_year"::varchar as payment_method_card_exp_year
        ,charges.payment_method_details:"card":"fingerprint"::varchar as payment_method_card_fingerprint
        ,charges.payment_method_details:"card":"funding"::varchar as payment_method_card_funding
        ,charges.payment_method_details:"card":"last4"::varchar as payment_method_card_last4
        ,charges.payment_method_details:"card":"three_d_secure"::varchar as payment_method_card_three_d_secure
        ,charges.payment_method_details:"card":"wallet"::varchar as payment_method_card_wallet
        ,charges.payment_method_details:"type"::varchar as payment_method_type
        ,charges.refunded
        ,charges.refunds
        ,charges.source:"address_city"::varchar as source_address_city
        ,charges.source:"address_country"::varchar as source_address_country
        ,charges.source:"address_line1"::varchar as source_address_line1
        ,charges.source:"address_line1_check"::varchar as source_address_line1_check
        ,charges.source:"address_line2"::varchar as source_address_line2
        ,charges.source:"address_state"::varchar as source_address_state
        ,charges.source:"address_zip"::varchar as source_address_zip
        ,charges.source:"address_zip_check"::varchar as source_address_zip_check
        ,charges.source:"brand"::varchar as source_brand
        ,charges.source:"country"::varchar as source_country
        ,charges.source:"customer"::varchar as source_customer
        ,charges.source:"cvc_check"::varchar as source_cvc_check
        ,charges.source:"dynamic_last4"::varchar as source_dynamic_last4
        ,charges.source:"exp_month"::varchar as source_exp_month
        ,charges.source:"exp_year"::varchar as source_exp_year
        ,charges.source:"fingerprint"::varchar as source_fingerprint
        ,charges.source:"funding"::varchar as source_funding
        ,charges.source:"id"::varchar as source_id
        ,charges.source:"last4"::varchar as source_last4
        ,charges.source:"name"::varchar as source_name
        ,charges.source:"object"::varchar as source_object
        ,charges.source:"tokenization_method"::varchar as source_tokenization_method
        ,charges.status
        ,charges.updated
        ,charges.invoice
        ,charges.description
        ,charges.receipt_number
    FROM {{ source('stripe_raw','charges') }}
)

select * from charges