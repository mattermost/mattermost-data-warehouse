with source as (

    select * from {{ source('stripe', 'invoices') }}

),

invoices as (

    select
            account_country as account_country
          , account_name as account_name
          , amount_due as amount_due
          , amount_paid as amount_paid
          , amount_remaining as amount_remaining
          , attempted as attempted
          , attempt_count as attempt_count
          , automatic_tax as automatic_tax
          , auto_advance as auto_advance
          , billing_reason as billing_reason
          , charge as charge_id
          , collection_method as collection_method
          , created as created_at
          , currency as currency
          , customer as customer
          , customer_address as customer_address
          , customer_email as customer_email
          , customer_name as customer_name
          , customer_shipping:"address":"line1"::string as line1
          , customer_shipping:"address":"line2"::string as line2
          , customer_shipping:"address":"postal_code"::string as postal_code
          , customer_shipping:"address":"city"::string as city
          , customer_shipping:"address":"state"::string as state
          , customer_shipping:"address":"country"::string as country
          , customer_shipping:"name"::string as customer_full_name
          , customer_tax_exempt as customer_tax_exempt
          , customer_tax_ids as customer_tax_ids
          , default_tax_rates as default_tax_rates
          , discount as discount
          , discounts as discounts
          , due_date as due_date_at
          , ending_balance as ending_balance
          , footer as footer
          , hosted_invoice_url as hosted_invoice_url
          , id as invoice_id
          , invoice_pdf as invoice_pdf
          , lines as lines
          , livemode as livemode
          , number as number
          , object as object
          , paid as paid
          , paid_out_of_band as paid_out_of_band
          , payment_intent as payment_intent
          , payment_settings as payment_settings
          , period_end as period_end_at
          , period_start as period_start_at
          , post_payment_credit_notes_amount as post_payment_credit_notes_amount
          , pre_payment_credit_notes_amount as pre_payment_credit_notes_amount
          , receipt_number as receipt_number
          , starting_balance as starting_balance
          , status as status
          , status_transitions as status_transitions
          , subscription as subscription_id
          , subtotal as subtotal
          , tax as tax
          , total as total
          , total_discount_amounts as total_discount_amounts
          , total_tax_amounts as total_tax_amounts
          , updated as updated_at
          , webhooks_delivered_at as webhooks_delivered_at
          , next_payment_attempt as next_payment_attempt
          , payment as payment
          , forgiven as forgiven
          , billing as billing
          , finalized_at as finalized_at
          , closed as closed_at
          , updated_by_event_type as updated_by_event_type
          , date as date

    from source

)

select * from invoices