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
        to_timestamp_ntz(date_part(epoch_microsecond, license_end_at)) as expire_at
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
)

select license_id, expire_at, 'CWS' as source from {{ ref('stg_cws__license') }}
union
select license_id, expire_at, 'Stripe' as source from stripe_licenses
union
select license_id, expire_at, 'Rudderstack' as source from {{ ref('stg_mm_telemetry_prod__license') }} where license_id is not null
union
select license_id, expire_at, 'Segment' as source from  {{ ref('stg_mattermost2__license') }} where license_id is not null
union
select license_id, expire_at as expire_at, 'Legacy' as source from {{ ref('stg_licenses__licenses') }}
