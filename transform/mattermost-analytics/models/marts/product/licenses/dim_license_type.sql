select
    {{ dbt_utils.generate_surrogate_key(['product_id']) }} as license_type_id,
    sku,
    name,
    cws_product_family as product_family
from
    {{ ref('stg_stripe__products') }}

union

select
    'Unknown' as license_type_id,
    'Unknown' as sku,
    'Unknown' as name,
    'Unknown' as product_family