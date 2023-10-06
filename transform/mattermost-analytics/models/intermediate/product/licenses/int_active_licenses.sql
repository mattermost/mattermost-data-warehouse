-- Temporarily materialize during development
{{
    config({
        "materialized": "table",
        "snowflake_warehouse": "transform_l"
    })
}}
with stripe_licenses as (
    select
        s.license_id,
        -- Convert to epoch in order to remove timezone
        to_timestamp_ntz(date_part(epoch_second, license_end_at)) as expire_at
    from
        {{ ref('stg_stripe__subscriptions')}} s
        left join {{ ref('stg_stripe__subscription_items')}} si on s.subscription_id = si.subscription_id
        left join {{ ref('stg_stripe__products')}} p on si.product_id = p.product_id
    where
        -- Onprem subscription/subscription items
        p.name not ilike '%cloud%'
        -- Skip support subscription items and focus on main plan
        and p.name <> 'Premier Support'
        -- Skip incomplete subscriptions
        and s.status <> 'incomplete_expired'
        -- Data before this date might not be in-line with specification
        and s.created_at > '2021-04-01'
        -- Ignore invalid records
        and s.license_id is not null
), all_licenses as (
    select license_id, expire_at, 'CWS' as source from {{ ref('stg_cws__license') }}
    union
    select license_id, expire_at, 'Stripe' as source from stripe_licenses
    union
    select license_id, expire_at, 'Rudderstack' as source from {{ ref('stg_mm_telemetry_prod__license') }} where license_id is not null and license_name in ('E10', 'E20', 'enterprise', 'professional')
    union
    select license_id, expire_at, 'Segment' as source from  {{ ref('stg_mattermost2__license') }} where license_id is not null and license_name in ('E10', 'E20', 'enterprise', 'professional')
    -- Legacy licenses
    union
    select license_id, expire_at, 'Legacy' as source from {{ ref('stg_licenses__licenses') }}
), distinct_licenses as (
    -- Licenses with more than one expiration date
    select
        license_id,
        count(distinct expire_at) as count_expiration_dates,
        array_agg(source) within group (order by source) as sources
    from
        all_licenses
    where
        license_id is not null
    group by 1
)
select
    all_licenses.license_id,
    all_licenses.expire_at,
    all_licenses.source,

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
    sourecs,
    -- Mark license IDs with > 1 expiration dates as outliers
    outliers.count_expiration_dates > 1 as is_outlier
from
    distinct_licenses
    left join {{ ref('stg_cws__license') }} cws on all_licenses.license_id = cws.license_id
