-- Temporarily materialize during development
{{
    config({
        "materialized": "table",
        "snowflake_warehouse": "transform_l"
    })
}}

with active_licenses as (

     select
        license_id,
        count(distinct expire_at) > 1 as has_multiple_expiration_dates_across_sources,
        array_sort(array_agg(distinct source)) as sources
    from {{ ref('int_licenses_per_source') }}
    group by 1
    having
        max(expire_at) >= current_date
)
select
    active_licenses.license_id,

    -- License data
    coalesce(cws.issued_at, rudder.issued_at) as issued_at,
    coalesce(cws.starts_at, rudder.starts_at) as starts_at,
    coalesce(cws.expire_at, rudder.expire_at) as expire_at,
    cws.stripe_product_id,
    coalesce(cws.sku_short_name, get(rudder.license_names, 0)) as license_name,
    cws.is_gov_sku,
    cws.is_trial,
    cws.company_name,
    cws.customer_email,
    cws.customer_name,
    cws.customer_id as cws_customer_id,
    coalesce(cws.licensed_seats, rudder.licensed_seats) as licensed_seats,
    cws.created_at,
    datediff(day, coalesce(cws.starts_at, rudder.starts_at), coalesce(cws.expire_at, rudder.expire_at)) as duration_days,
    datediff(month, coalesce(cws.starts_at, rudder.starts_at), coalesce(cws.expire_at, rudder.expire_at)) as duration_months,

    -- Mark where the datasources reporting each license
    active_licenses.sources,
    -- Mark license IDs with > 1 expiration dates on all sources as outliers
    active_licenses.has_multiple_expiration_dates_across_sources
from
    active_licenses
    left join {{ ref('stg_cws__license') }} cws on active_licenses.license_id = cws.license_id
    left join {{ ref('int_rudder_licenses_deduped') }} rudder on active_licenses.license_id = rudder.license_id
