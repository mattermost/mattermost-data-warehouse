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
        max(has_multiple_expiration_dates) as has_multiple_expiration_dates_per_source,
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
    cws.issued_at,
    cws.starts_at,
    cws.expire_at,
    cws.sku,
    cws.sku_short_name,
    cws.is_gov_sku,
    cws.is_trial,
    cws.company_name,
    cws.customer_email,
    cws.customer_name,
    cws.number_of_users,
    cws.created_at,
    datediff(day, cws.starts_at::date, cws.expire_at::date) as duration_days,
    datediff(month, cws.starts_at::date, cws.expire_at::date) as duration_months,

    -- Mark where the datasources reporting each license
    active_licenses.sources,
    -- Mark license IDs with > 1 expiration dates on single source as outliers
    active_licenses.has_multiple_expiration_dates_per_source,
    -- Mark license IDs with > 1 expiration dates on all sources as outliers
    active_licenses.has_multiple_expiration_dates_across_sources
from
    active_licenses
    left join {{ ref('stg_cws__license') }} cws on active_licenses.license_id = cws.license_id
