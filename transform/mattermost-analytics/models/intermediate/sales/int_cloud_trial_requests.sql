{{
    config({
        "materialized": "table"
    })
}}

WITH customers as (
    SELECT
        customer_id,
        email,
        name as customer_name
    FROM
        {{ ref('stg_stripe__customers') }}
    where
        created_at >= '2023-04-27' -- only select customers after the release.
),
subscriptions as (
    SELECT
        subscription_id,
        trial_start_at,
        trial_end_at,
        product_id
    FROM
        {{ ref('stg_stripe__subscriptions') }}
),
products as (
    SELECT
        product_id,
        name as product_name,
        sku as product_sku
    FROM
        {{ ref('stg_stripe__products') }}
),
customers_with_cloud_enterprise_trial as (
    select
        customers.*,
        subscriptions.*,
        products.*
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
    customers_with_cloud_enterprise_trial;
