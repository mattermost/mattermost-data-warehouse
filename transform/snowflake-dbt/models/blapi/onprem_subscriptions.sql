{{config({
    "schema": "blapi",
    "unique_key":"id",
    "tags":["hourly","blapi"]
  })
}}

WITH latest_payment AS (
    SELECT
        payments.stripe_charge_id,
        payments.subscription_id,
        'ONL' || payments.invoice_number AS invoice_number,
        ROW_NUMBER() OVER (PARTITION BY payments.subscription_id ORDER BY payments.created_at DESC) as row_num
    FROM {{ ref('payments') }}
), subscriptions AS (
    SELECT
        s.*,
        p.sku,
        latest_payment.stripe_charge_id,
        latest_payment.invoice_number
    FROM {{ source('blapi', 'subscriptions_version') }} s
    JOIN {{ source('blapi', 'products') }} p ON s.product_id = p.id
    JOIN latest_payment ON s.id = latest_payment.subscription_id AND latest_payment.row_num = 1
    WHERE s.subscription_version_id_mod
        AND p.name != 'Mattermost Cloud'
)
SELECT * FROM subscriptions