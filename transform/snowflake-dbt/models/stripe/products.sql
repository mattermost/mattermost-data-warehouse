{{config({
    "materialized": 'table',
    "schema": "stripe"
  })
}}

WITH products AS (
    SELECT 
        products.active
        ,products.attributes
        ,products.created
        ,products.id
        ,products.images
        ,products.livemode
        ,products.metadata:"cws-product-family"::varchar as cws_product_family
        ,products.metadata:"cws-sku-name"::varchar as cws_sku_name
        ,products.metadata:"cws-allowable-addons"::varchar as cws_allowable_addons
        ,products.name
        ,products.type
        ,products.updated
        ,products.unit_label
    FROM {{ source('stripe_raw','products') }}
)

select * from products