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
        TRIM(COALESCE(metadata:"contactfirstname"::varchar, 
        customers.metadata:"cws-additional-contact-first-name"::varchar),'"') as contact_first_name,
        TRIM(COALESCE(metadata:"contactlastname"::varchar, 
        customers.metadata:"cws-additional-contact-last-name"::varchar),'"') as contact_last_name,
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