{{
  config({
    "schema": "blapi",
    "unique_key":"id",
    "tags": ["hourly", "blapi", "deprecated"]
  })
}}

WITH freemium_subscriptions AS (
    SELECT
        s.*,
        p.sku,
        su.status,
        INITCAP(SPLIT_PART(replace(su.cws_dns, '-', ' '), '.', 1)) as company,
        charge as stripe_charge_id,
        invoices.id as invoice_number,
        ROW_NUMBER() OVER (PARTITION BY s.id ORDER BY s.transaction_id DESC) as row_num
    FROM {{ source('blapi', 'subscriptions_version') }} s
    JOIN {{ source('stripe', 'subscriptions') }} su on su.id = s.stripe_id
    JOIN {{ source('blapi', 'products') }} p ON s.product_id = p.id
    LEFT JOIN {{ source('stripe', 'invoices') }} on invoices.subscription = su.id
    WHERE s.cloud_dns is not null
    AND su.date_converted_to_paid is not null
)

SELECT * FROM freemium_subscriptions
WHERE row_num = 1
