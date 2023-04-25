{{
    config({
        "materialized": "table"
    })
}}

WITH customers as (
    SELECT
        { { dbt_utils.star(ref('stg_stripe__customers')) } }
    FROM
        { { ref('stg_stripe__customers') } }
    where
        created >= '2023-04-22' -- to be removed
),
subscriptions as (
    SELECT
        { { dbt_utils.star(ref('stg_stripe__subscriptions')) } }
    FROM
        { { ref('stg_stripe__subscriptions') } }
),
products as (
    SELECT
        { { dbt_utils.star(ref('stg_stripe__products')) } }
    FROM
        { { ref('stg_stripe__products') } }
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
        AND products.sku = `cloud-enterprise` -- TBD if we need this after yesterday's call with Nick.
)
select
    *
from
    customers_with_cloud_enterprise_trial

-- {% if is_incremental() %}
--     WHERE received_at > (SELECT MAX(received_at) FROM {{ this }}) 
--     {% endif %}