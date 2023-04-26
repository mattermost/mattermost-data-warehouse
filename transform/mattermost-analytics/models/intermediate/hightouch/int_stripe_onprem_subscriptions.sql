{{config({
    "materialized": 'table',
    "unique_key":"invoice_id",
  })
}}

WITH payment AS (
    SELECT
        invoices.charge,
        invoices.subscription,
        'ONL' || invoices.number AS invoice_number,
        invoices.number as stripe_invoice_number,
        invoices.id as invoice_id,
        invoices.created_at as invoice_created_at,
        ROW_NUMBER() OVER (PARTITION BY invoices.subscription ORDER BY invoices.created_at) as row_num
    FROM {{ ref('stg_stripe__invoices') }} invoices
), subscriptions AS (
    SELECT
        p.sku as sku,
        payment.charge as stripe_charge_id,
        payment.invoice_number,
        payment.stripe_invoice_number,
        ili.quantity as invoice_quantity,
        ili.amount as invoice_amount,
        ROW_NUMBER() OVER (PARTITION BY s.subscription_id, payment.stripe_invoice_number order by payment.invoice_created_at) as row_num
        , s.*
    FROM {{ ref('stg_stripe__subscriptions') }} s
    JOIN {{ ref('stg_stripe__products') }} p ON s.product = p.product_id
    JOIN payment ON s.subscription_id = payment.subscription 
    JOIN {{ ref('stg_stripe__invoice_line_items') }} ili ON payment.invoice_id = ili.invoice
    WHERE p.name not like '%Cloud%' and amount > 0
)
SELECT * FROM subscriptions