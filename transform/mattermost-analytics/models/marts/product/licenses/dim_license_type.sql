select
    {{ dbt_utils.generate_surrogate_key(['product_id']) }} as license_type_id,
    sku,
    name,
    cws_product_family as product_family
from
    {{ ref('stg_stripe_products') }}