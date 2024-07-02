with source as (

    select * from {{ source('stripe', 'products') }}

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
        unit_label,

    from source

)

select * from products