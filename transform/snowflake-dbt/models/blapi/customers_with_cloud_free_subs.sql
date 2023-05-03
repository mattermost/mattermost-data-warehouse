{{config({
    "schema": "hightouch",
    "unique_key":"id",
    "tags":["hourly","blapi"]
  })
}}
-- customer info along with current subscriptions and previous subscriptions
with current_subscriptions AS(
    SELECT 
        *
    FROM       
        {{ ref('cloud_free_subscriptions') }}
        WHERE row_num = 1
), previous_subscriptions_pre AS(
    SELECT 
        id as current_subscription_id, 
        lag(coalesce(plan:"product"::varchar, metadata:"current_product_id"::varchar), 1) over (order by customer,created desc) as previous_product_id
    FROM 
        {{ ref('cloud_free_subscriptions') }}
), previous_subscriptions AS (
    SELECT 
        current_subscription_id, 
        products.cws_sku_name as previous_product_sku
    FROM 
        previous_subscriptions_pre
        JOIN {{ source('stripe', 'products') }} on products.id = previous_product_id
), customers_with_free_subs AS (
    SELECT
        customers.id as customer_id,
        customers.email,
        customers_blapi.first_name,
        coalesce(NULLIF(TRIM(customers_blapi.last_name), ''), customers.email) as last_name,
        COALESCE(SPLIT_PART(cloud_subscriptions.cloud_dns, '.', 1), SPLIT_PART(customers.email, '@', 2)) as domain,
        current_subscriptions.id as subscription_id,
        current_subscriptions.cws_dns as cloud_dns,
        current_subscriptions.product_sku,
        previous_subscriptions.previous_product_sku,
        left(coalesce(current_subscriptions.company, customers.email), 40) as company_name,
        current_subscriptions.status,
        current_subscriptions.created >= '2022-06-14' as hightouch_sync_eligible
    FROM {{ source('stripe','customers') }} 
        JOIN current_subscriptions ON customers.id = current_subscriptions.customer
        LEFT JOIN previous_subscriptions ON current_subscriptions.id = previous_subscriptions.current_subscription_id
        LEFT JOIN {{ source('blapi', 'customers') }} customers_blapi ON customers_blapi.stripe_id = customers.id
)
select * from customers_with_free_subs
