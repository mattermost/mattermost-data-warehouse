{{
    config({
        "materialized": "table"
    })
}}

WITH customers as (
    SELECT
        customer_id,
        email,
        name,
        contact_first_name,
        contact_last_name
    FROM
        {{ ref('stg_stripe__customers') }}
    where
        created_at >= '2023-03-27' -- only select customers after the release.
),
subscriptions as (
    SELECT
        subscription_id,
        customer_id,
        trial_start_at,
        trial_end_at,
        product_id
    FROM
        {{ ref('stg_stripe__subscriptions') }}
),
products as (
    SELECT
        product_id,
        name,
        sku
    FROM
        {{ ref('stg_stripe__products') }}
),
customers_with_cloud_enterprise_trial as (
    select
        customers.customer_id,
        customers.email,
        customers.name as customer_name,
        subscriptions.subscription_id,
        subscriptions.trial_start_at,
        subscriptions.trial_end_at,
        products.product_id,
        products.name as product_name
    from
        customers
        left join subscriptions on subscriptions.customer_id = customers.customer_id
        left join products on subscriptions.product_id = products.product_id
        where CURRENT_DATE < subscriptions.trial_end_at
        AND products.sku = 'cloud-enterprise' -- TBD if we need this after yesterday's call with Nick.
)

select
    *
from
    customers_with_cloud_enterprise_trial
