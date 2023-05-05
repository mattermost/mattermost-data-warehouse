with source as (

    select * from {{ source('stripe_raw', 'customers') }}

),

customers as (

    select
        account_balance,
        balance,
        cards,
        created as created_at,
        currency,
        delinquent,
        email,
        id as customer_id,
        invoice_prefix,
        invoice_settings,
        livemode,
        COALESCE(metadata:"contactfirstname"::string, 
        metadata:"cws-additional-contact-first-name"::string) as contact_first_name,
        COALESCE(metadata:"contactlastname"::string, 
        metadata:"cws-additional-contact-last-name"::string) as contact_last_name,
        metadata:"cws-customer"::varchar as portal_customer_id,
        metadata,
        name,
        next_invoice_sequence,
        preferred_locales,
        sources,
        subscriptions,
        tax_exempt,
        updated as updated_at,
        default_source,
        shipping,
        tax_ids,
        address,
        default_card,
        discount,
        updated_by_event_type

    from source

)

select * from customers