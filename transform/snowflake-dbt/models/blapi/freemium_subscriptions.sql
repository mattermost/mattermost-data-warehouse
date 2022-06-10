{{config({
    "schema": "blapi",
    "unique_key":"id",
    "tags":["hourly","blapi"]
  })
}}

WITH freemium_subscriptions AS (
    SELECT
        s.*,
        p.sku,
        charge as stripe_charge_id,
        invoices.id as invoice_number,
        ROW_NUMBER() OVER (PARTITION BY s.id ORDER BY s.transaction_id DESC) as row_num
    FROM {{ source('blapi', 'subscriptions_version') }} s
    JOIN {{ source('stripe', 'subscriptions') }} su on su.id = s.stripe_id
    JOIN {{ source('blapi', 'products') }} p ON s.product_id = p.id
    LEFT JOIN {{ source('stripe', 'invoices') }} on invoices.subscription = su.id
    WHERE
        -- p.name in ('Mattermost Cloud', 'Cloud Enterprise', 'Cloud Starter', 'Cloud Professional')
        s.cloud_dns is not null
        AND su.date_converted_to_paid is not null
)

SELECT * FROM freemium_subscriptions
WHERE row_num = 1
