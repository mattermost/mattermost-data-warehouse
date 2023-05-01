{{config({
    "materialized": 'table',
    "unique_key":"invoice_id",
  })
}}


WITH subscriptions AS (
select s.subscription_id
    , s.purchase_order_number
    , s.stripe_customer_id 
    , s.license_id
    , s.edition
    , s.product
    , p.sku
    FROM {{ ref('stg_stripe__subscriptions') }}  s 
    JOIN {{ ref('stg_stripe__products') }} p ON s.product = p.product_id
    where s.edition not ilike '%cloud%' 
), invoices AS (
    SELECT s.*
        , invoices.charge_id
        , invoices.subscription
        , 'ONL' || invoices.number AS invoice_number
        , invoices.number as stripe_invoice_number
        , invoices.invoice_id as invoice_id
        , invoices.created_at as invoice_created_at
        , invoices.customer_email AS email
        , SPLIT_PART(invoices.customer_email, '@', 2) as domain
        , invoices.customer_name
        , invoices.line1
        , invoices.line2
        , invoices.postal_code
        , invoices.city
        , invoices.state
        , invoices.country
        , invoices.customer_full_name
        , invoices.total
        , invoices.subtotal
        , ROW_NUMBER() OVER (PARTITION BY invoices.subscription ORDER BY invoices.created_at) as invoice_row_num
    FROM {{ ref('stg_stripe__invoices') }} invoices
    JOIN subscriptions s on s.subscription_id = invoices.subscription
    WHERE invoices.status = 'paid'
) 
, invoice_line_items AS (
    SELECT i.* 
        , ili.quantity as quantity
        , LAG(ili.quantity) OVER (PARTITION BY i.subscription_id ORDER BY i.invoice_created_at) previous_quantity
        , ili.quantity - COALESCE(LAG(ili.quantity) OVER (PARTITION BY i.subscription ORDER BY i.invoice_created_at),0) seats_purchased
    from {{ ref('stg_stripe__invoice_line_items') }} ili 
    JOIN invoices i ON i.invoice_id = ili.invoice_id
    WHERE amount > 0
) select 
CASE WHEN invoice_row_num = 1 THEN 'New Purchase' 
     WHEN invoice_row_num > 1 THEN 'Expansion' 
     END AS opportunity_type
    , 'Online' as order_type
    , '{{ var("salesforce_default_ownerid") }}' AS ownerid
    , '6. Closed Won' as stagename
    , domain || ' ' || sku || ' qty:' || seats_purchased || ' inv:' || invoice_number AS opportunity_name
    , invoice_line_items.*    
from invoice_line_items
