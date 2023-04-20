{{config({
    "materialized": 'table',
    "schema": "stripe",
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
        invoices.created as invoice_created_at,
        ROW_NUMBER() OVER (PARTITION BY invoices.subscription ORDER BY invoices.created) as row_num
    FROM {{ source('stripe_raw','invoices') }} invoices
), subscriptions AS (
    SELECT
        p.cws_sku_name as sku,
        payment.charge as stripe_charge_id,
        payment.invoice_number,
        payment.stripe_invoice_number,
        ili.quantity as invoice_quantity,
        ili.amount as invoice_amount,
        ROW_NUMBER() OVER (PARTITION BY s.id, payment.stripe_invoice_number order by payment.invoice_created_at) as row_num
        , s.*
    FROM {{ source('stripe_raw','subscriptions') }} s
    JOIN {{ source('stripe_raw','products') }} p ON s.plan:"product" = p.id
    JOIN payment ON s.id = payment.subscription 
    JOIN {{ source('stripe_raw','invoice_line_items') }} ili ON payment.invoice_id = ili.invoice
    WHERE p.name not like '%Cloud%' and amount > 0
)
SELECT * FROM subscriptions