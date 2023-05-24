
with paid_transactions as (
    select
        s.customer_id as customer_id,
        s.subscription_id as subscription_id,
        i.invoice_id as invoice_id,
        i.created_at as invoice_created_at,
        lower(c.email) as customer_email,
        lower(i.customer_email) as invoice_email,
        i.number as invoice_number,
        i.amount_paid,
        i.total,
        s.product_id,
        p.name as product_name,
        s.license_id as license_id,
        s.cws_dns as cws_dns,
        s.cws_installation as cws_installation_id,
        row_number() over(partition by s.customer_id order by i.created_at asc) AS event_order_number
    from
        {{ ref('stg_stripe__invoices') }} i
        left join {{ ref('stg_stripe__subscriptions') }} s on s.subscription_id = i.subscription
        left join {{ ref('stg_stripe__customers') }} c on s.customer_id = c.customer_id
        left join {{ ref('stg_stripe__products') }} p on p.product_id = s.product_id
    where
        -- Only paid events are interesting
        i.total > 0
        -- TODO: ignore cloud free
)
select
    *
from paid_transactions
;


