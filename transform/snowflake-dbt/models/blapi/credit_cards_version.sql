{{config({
    "materialized": "table",
    "schema": "blapi"
  })
}}

WITH credit_cards_version AS (
    SELECT 
        credit_cards_version.id,
        credit_cards_version.fingerprint,
        credit_cards_version.stripe_id,
        credit_cards_version.card_brand,
        credit_cards_version.last4,
        credit_cards_version.exp_month,
        credit_cards_version.exp_year,
        credit_cards_version.address_check,
        credit_cards_version.cvc_check,
        credit_cards_version.zip_check,
        credit_cards_version.transaction_id,
        credit_cards_version.end_transaction_id,
        credit_cards_version.operation_type,
        credit_cards_version.fingerprint_mod,
        credit_cards_version.stripe_id_mod,
        credit_cards_version.card_brand_mod,
        credit_cards_version.last4_mod,
        credit_cards_version.exp_month_mod,
        credit_cards_version.exp_year_mod,
        credit_cards_version.address_check_mod,
        credit_cards_version.cvc_check_mod,
        credit_cards_version.zip_check_mod,
        credit_cards_version.three_d_secure_support,
        credit_cards_version.three_d_secure_support_mod,
        credit_cards_version.card_country,
        credit_cards_version.card_country_mod
    FROM {{ source('blapi','credit_cards') }}
    LEFT JOIN {{ source('blapi','credit_cards_version') }} ON credit_cards.id = credit_cards_version.id
)

SELECT * FROM credit_cards_version