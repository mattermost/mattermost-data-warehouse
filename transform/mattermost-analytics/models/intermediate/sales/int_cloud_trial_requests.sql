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
        created_at >= '2022-01-01' -- only select customers after the release.
),
subscriptions as (
    SELECT
        subscription_id,
        cws_installation,
        cws_dns,
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
cloud_trial_requests as (
    select
        customers.customer_id,
        customers.email,
        customers.name as customer_name,
        customers.contact_first_name as customer_first_name,
        customers.contact_last_name as customer_last_name,
        subscriptions.subscription_id,
        subscriptions.trial_start_at,
        subscriptions.trial_end_at,
        subscriptions.cws_installation,
        products.product_id,
        products.name as product_name
    from
        customers
        -- will lead to rows fanning out since a customer can have many subscriptions
        left join subscriptions on subscriptions.customer_id = customers.customer_id
        left join products on subscriptions.product_id = products.product_id
        -- where CURRENT_DATE < subscriptions.trial_end_at
        -- only get cloud subscriptions
        AND cws_installation is not null
        -- TBD if we need this after yesterday's call with Nick.
        AND products.sku = 'cloud-enterprise'
)

select
    *
from
    cloud_trial_requests
