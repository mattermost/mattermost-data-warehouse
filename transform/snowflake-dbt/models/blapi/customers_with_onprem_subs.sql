{{config({
    "schema": "hightouch",
    "unique_key":"id",
    "tags":["hourly","blapi"]
  })
}}

WITH latest_credit_card_address AS (
    SELECT
        payment_methods.customer_id,
        addresses.line1,
        addresses.line2,
        addresses.postal_code,
        addresses.city,
        coalesce(postal_code_mapping.state_code, addresses.state) as state,
        coalesce(postal_code_mapping.country, addresses.country) as country,
        ROW_NUMBER() OVER (PARTITION BY payment_methods.customer_id ORDER BY payment_methods.created_at DESC) as row_num
    FROM {{ ref('credit_cards') }}
    JOIN {{ ref('payment_methods') }} ON credit_cards.id = payment_methods.id
    JOIN {{ ref('addresses') }} ON payment_methods.address_id = addresses.id
    LEFT JOIN {{ source('util', 'postal_code_mapping') }}
        ON addresses.country = postal_code_mapping.country
        AND postal_code_mapping.postal_code like addresses.postal_code || '%'
    WHERE addresses.address_type = 'billing'
), customers_with_onprem_subs AS (
    SELECT
        customers.id as customer_id,
        UUID_STRING('78157189-82de-4f4d-9db3-88c601fbc22e', customers.id) AS account_external_id,
        UUID_STRING('78157189-82de-4f4d-9db3-88c601fbc22e', onprem_subscriptions.subscription_version_id) AS opportunity_external_id,
        UUID_STRING('78157189-82de-4f4d-9db3-88c601fbc22e', customers.id || customers.email) AS contact_external_id,
        customers.email,
        customers.first_name,
        customers.last_name,
        SPLIT_PART(customers.email, '@', 2) as domain,
        onprem_subscriptions.id as subscription_id,
        onprem_subscriptions.subscription_version_id,
        onprem_subscriptions.previous_subscription_version_id,
        onprem_subscriptions.start_date,
        onprem_subscriptions.end_date,
        onprem_subscriptions.total_in_cents / 100.0 as total,
        onprem_subscriptions.updated_at,
        onprem_subscriptions.invoice_number,
        onprem_subscriptions.stripe_charge_id,
        onprem_subscriptions.num_seats,
        onprem_subscriptions.sku,
        COALESCE(subscriptions.license_id, onprem_subscriptions.id) as license_key,
        subscriptions.purchase_order_num,
        latest_credit_card_address.line1,
        latest_credit_card_address.line2,
        latest_credit_card_address.postal_code,
        latest_credit_card_address.city,
        latest_credit_card_address.state,
        latest_credit_card_address.country
    FROM {{ ref('customers_blapi') }} customers
        JOIN {{ ref('onprem_subscriptions') }} ON customers.id = onprem_subscriptions.customer_id
        JOIN {{ ref('subscriptions') }} ON onprem_subscriptions.stripe_id = subscriptions.id
        JOIN latest_credit_card_address
            ON customers.id = latest_credit_card_address.customer_id
            AND latest_credit_card_address.row_num = 1
)
SELECT * FROM customers_with_onprem_subs