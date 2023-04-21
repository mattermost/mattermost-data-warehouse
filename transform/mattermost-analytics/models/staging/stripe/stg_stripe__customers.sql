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
        metadata:"contactfirstname"::varchar as contact_first_name,
        metadata:"contactlastname"::varchar as contact_last_name,
        metadata:"cws-customer"::varchar as portal_customer_id,
        metadata,
        name,
        next_invoice_sequence,
        preferred_locales,
        sources,
        subscriptions,
        tax_exempt,
        updated as updated_at,
        _sdc_batched_at,
        _sdc_extracted_at,
        _sdc_received_at,
        _sdc_sequence,
        _sdc_table_version,
        default_source,
        shipping,
        tax_ids,
        address,
        default_card,
        discount,
        updated_by_event_type,
        updated as _updated_at,
        created as _created_at

    from source

)

select * from customers