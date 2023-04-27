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
        cws_installation,
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
        products.product_id,
        products.name as product_name
    from
        customers
        left join subscriptions on subscriptions.customer_id = customers.customer_id
        left join products on subscriptions.product_id = products.product_id
        where CURRENT_DATE < subscriptions.trial_end_at
        AND cws_installation is not null -- only get cloud subscriptions
        AND products.sku = 'cloud-enterprise' -- TBD if we need this after yesterday's call with Nick.
)

select
    *
from
    cloud_trial_requests
