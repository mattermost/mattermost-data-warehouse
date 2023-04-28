with source as (

    select * from {{ source('stripe_raw', 'invoice_line_items') }}

),

invoice_line_items as (

    select
          amount as amount
        , currency as currency
        , description as description
        , discountable as discountable
        , discounts as discounts
        , discount_amounts as discount_amounts
        , id as invoice_line_item_id
        , invoice as invoice
        , invoice_item as invoice_item
        , livemode as livemode
        , object as object
        , period as period
        , plan as plan
        , price as price
        , proration as proration
        , proration_details as proration_details
        , quantity as quantity
        , subscription as subscription
        , subscription_item as subscription_item
        , tax_amounts as tax_amounts
        , tax_rates as tax_rates
        , type as type

    from source

)

select * from invoice_line_items