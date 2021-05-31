{{config({
    "schema": "hightouch",
    "materialized": "view",
    "tags":["hourly","blapi"]
  })
}}

WITH subscriptions_with_products AS (
    SELECT
        customers_with_onprem_subs.*,
        products.pricebookentryid
    FROM {{ ref('customers_with_onprem_subs') }}
    JOIN {{ source('blapi', 'products') }} ON customers_with_onprem_subs.sku = products.sku
), onprem_olis_to_sync AS (
    SELECT
        subscriptions_with_products.*,
        'Recurring' AS product_type,
        'New' AS product_line_type,
        'Discount' AS pricing_method,
        0 AS zero_amount,
        subscriptions_with_products.total / subscriptions_with_products.num_seats AS list_price,
        UUID_STRING(
            '78157189-82de-4f4d-9db3-88c601fbc22e',
            subscriptions_with_products.opportunity_external_id || 'oli')
        AS oli_external_id
    FROM subscriptions_with_products
    LEFT JOIN {{ ref('opportunity') }}
        ON subscriptions_with_products.opportunity_external_id = opportunity.dwh_external_id__c
            OR subscriptions_with_products.stripe_charge_id = opportunity.stripe_id__c
    LEFT JOIN {{ ref('opportunitylineitem') }}
        ON subscriptions_with_products.subscription_version_id = opportunitylineitem.subs_version_id__c
    LEFT JOIN {{ ref('account') }}
        ON opportunity.accountid = account.id
    WHERE opportunity.id IS NULL AND opportunitylineitem.id IS NULL
)
SELECT * FROM onprem_olis_to_sync