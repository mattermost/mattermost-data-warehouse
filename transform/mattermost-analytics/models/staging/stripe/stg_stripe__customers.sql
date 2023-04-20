with source as (

    select * from {{ source('stripe_raw', 'customers') }}

),

renamed as (

    select
        account_balance,
        balance,
        cards,
        created,
        currency,
        delinquent,
        email,
        id,
        invoice_prefix,
        invoice_settings,
        livemode,
        metadata:"contactfirstname"::varchar as contactfirstname,
        metadata:"contactlastname"::varchar as contactlastname,
        metadata:"cws-customer"::varchar as cws_customer,
        metadata,
        name,
        next_invoice_sequence,
        object,
        preferred_locales,
        sources,
        subscriptions,
        tax_exempt,
        updated,
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
        updated_by_event_type

    from source

)

select * from renamed