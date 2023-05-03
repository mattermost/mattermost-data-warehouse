{{config({
    "schema": "hightouch",
    "unique_key":"id",
    "tags":["hourly","blapi"]
  })
}}
-- customer info along with current paid subscriptions
-- professional subscriptions with status cancelled and active will be considered as new subscriptions. Total = active + cancelled
with current_subscriptions AS(
    SELECT 
        *,
        cloud_paid_subscriptions."START" as start_date
    FROM       
        {{ ref('cloud_paid_subscriptions') }}
        WHERE row_num = 1
), customers_with_paid_subs AS (
    SELECT
        customers.id as customer_id,
        UUID_STRING(
                '78157189-82de-4f4d-9db3-88c601fbc22e',
                customers.id)
        AS account_external_id,
        UUID_STRING(
                '78157189-82de-4f4d-9db3-88c601fbc22e',
                current_subscriptions.id)
        AS opportunity_external_id,
        customers.email,
        customers_blapi.first_name,
        coalesce(NULLIF(TRIM(customers_blapi.last_name), ''), customers.email) as last_name,
        COALESCE(SPLIT_PART(cloud_subscriptions.cloud_dns, '.', 1), SPLIT_PART(customers.email, '@', 2)) as domain,
        invoices.id as invoice_id,
        current_subscriptions.id as subscription_id,
        current_subscriptions.cws_dns as cloud_dns,
        current_subscriptions.name as sku,
        current_subscriptions.company,
        left(coalesce(current_subscriptions.company, customers.email), 40) as company_name,
        current_subscriptions.status,
        TO_DATE(current_subscriptions.date_converted_to_paid) as start_date,
        to_varchar(DATEADD(month, 1, current_subscriptions.start_date), 'yyyy-MM-dd HH:mm:ss.SSS Z') as end_date, -- only for cloud professional subscriptions
        current_subscriptions.created >= '2022-06-14' as hightouch_sync_eligible,
        ROW_NUMBER() OVER (PARTITION BY current_subscriptions.id ORDER BY invoices.created DESC) as row_num
    FROM {{ source('stripe','customers') }} 
        JOIN current_subscriptions ON customers.id = current_subscriptions.customer
        LEFT JOIN {{ source('stripe', 'invoices') }} ON invoices.subscription = current_subscriptions.id
        LEFT JOIN {{ source('blapi', 'customers') }} customers_blapi ON customers_blapi.stripe_id = customers.id
)
select * from customers_with_paid_subs where row_num = 1
