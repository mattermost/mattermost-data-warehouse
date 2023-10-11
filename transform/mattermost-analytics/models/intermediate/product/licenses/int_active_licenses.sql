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
), rudder_licenses as (
    select
        distinct
            license_id,
            issued_at,
            starts_at,
            expire_at,
            license_name,
            licensed_seats
        from
            {{ ref('stg_mm_telemetry_prod__license') }}
        where license_id is not null
)
select
    active_licenses.license_id,

    -- License data
    coalesce(cws.issued_at, rudder.issued_at),
    coalesce(cws.starts_at, rudder.starts_at),
    coalesce(cws.expire_at, rudder.expire_at),
    cws.sku,
    coalesce(cws.sku_short_name, rudder.license_name),
    cws.is_gov_sku,
    cws.is_trial,
    cws.company_name,
    cws.customer_email,
    cws.customer_name,
    coalesce(cws.licensed_seats, rudder.licensed_seats),
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
    left join rudder_licenses rudder on active_licenses.license_id = rudder.license_id
