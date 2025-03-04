WITH customers as (
    SELECT
        customer_id,
        email
    FROM
        {{ ref('stg_stripe__customers') }}
    where
        created_at >= '2023-04-27' -- only select customers after the release.
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
        subscriptions.subscription_id,
        subscriptions.trial_start_at,
        subscriptions.trial_end_at,
        subscriptions.cws_installation,
        subscriptions.cws_dns,
        products.product_id,
        products.name as product_name
    from
        customers
        -- Will lead to rows fanning out since a customer can have many subscriptions
        left join subscriptions on subscriptions.customer_id = customers.customer_id
        left join products on subscriptions.product_id = products.product_id
        -- Trial data end is in the future
        where CURRENT_DATE < subscriptions.trial_end_at
        -- Only get cloud subscriptions
        AND cws_installation is not null
)

select
    *
from
    cloud_trial_requests
