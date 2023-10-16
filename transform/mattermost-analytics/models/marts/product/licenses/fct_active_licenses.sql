select
    -- IDs
    license_id,
    case
        -- Handle licenses not in stripe
        when stripe_product_id is not null then {{ dbt_utils.generate_surrogate_key(['stripe_product_id']) }}
        else 'Unknown'
    end as license_type_id,
    {{ dbt_utils.generate_surrogate_key(['cws_customer_id', 'license_id']) }} as customer_id,
    -- Timestamps
    issued_at,
    starts_at,
    expire_at,
    created_at,

    -- Facts
    is_gov_sku,
    is_trial,
    licensed_seats,
    duration_days,
    duration_months,

    -- Metadata
    has_multiple_expiration_dates_across_sources
from
    {{ ref('int_active_licenses') }}
where
    ARRAY_CONTAINS('CWS'::variant, sources)