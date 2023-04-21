with source as (

    select * from {{ source('stripe_raw', 'products') }}

),

products as (

    select
        active,
        attributes,
        created AS created_at,
        description,
        id as product_id,
        images,
        livemode,
        metadata:"cws-product-family"::varchar as cws_product_family,
        metadata:"cws-sku-name"::varchar as sku,
        name,
        shippable,
        type,
        updated AS updated_at,
        _sdc_batched_at,
        _sdc_extracted_at,
        _sdc_received_at,
        _sdc_sequence,
        _sdc_table_version,
        unit_label,
        updated_by_event_type,
        deactivate_on

    from source

)

select * from products