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
)
select
    'stripe:' || subscriptions.id as activity_id,
    subscriptions.created_at as timestamp
    'Trial Requested' as activity,
    'stripe' as source,
    customers.customer_id as customer_id,
    customers.email,
    subscriptions.trial_start_at,
    subscriptions.trial_end_at,
    subscriptions.cws_installation as installation_id,
    null as server_id
from
    customers
    -- Will lead to rows fanning out since a customer can have many subscriptions
    left join subscriptions on subscriptions.customer_id = customers.customer_id
    left join products on subscriptions.product_id = products.product_id
where
    -- Only get trial subscriptions
    subscriptions.trial_start_at is not null
    -- Only get cloud subscriptions
    AND cws_installation is not null