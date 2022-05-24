{{config({
    "materialized": 'table',
    "schema": "stripe"
  })
}}

WITH invoices AS (
    SELECT 
        invoices.amount_due
        ,invoices.amount_paid
        ,invoices.amount_remaining
        ,invoices.attempted
        ,invoices.attempt_count
        ,invoices.auto_advance
        ,invoices.billing
        ,invoices.billing_reason
        ,invoices.charge
        ,invoices.closed
        ,invoices.created
        ,invoices.currency
        ,invoices.customer
        ,invoices.date
        ,invoices.due_date
        ,invoices.ending_balance
        ,invoices.forgiven
        ,invoices.hosted_invoice_url
        ,invoices.id
        ,invoices.invoice_pdf
        ,invoices.livemode
        ,invoices.number
        ,invoices.object
        ,invoices.paid
        ,invoices.payment
        ,invoices.period_end
        ,invoices.period_start
        ,invoices.starting_balance
        ,invoices.status
        ,invoices.subscription
        ,invoices.subtotal
        ,invoices.total
        ,invoices.updated
        ,invoices.webhooks_delivered_at
    FROM {{ source('stripe_raw','invoices') }}
)

select * from invoices