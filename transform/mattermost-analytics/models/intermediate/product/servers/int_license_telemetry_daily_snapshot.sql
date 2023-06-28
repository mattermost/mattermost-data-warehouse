{{
    config({
        "materialized": "table",
    })
}}
select
    coalesce(s.id, l.id) as daily_server_id,
    coalesce(s.id, l.server_id) as server_id,
    coalesce(s.server_date, l.server_date) as snapshot_date,
    coalese(s.customer_id, customer_id) as customer_id,
    coalese(s.license_id, license_id) as license_id,
    coalese(s.sku_short_name, sku_short_name) as sku_short_name,
    coalese(s.issued_at, issued_at) as issued_at,
    coalese(s.expire_at, expire_at) as expire_at,
    coalese(s.users, users) as users
from
    {{ ref('int_license_legacy_latest_daily') }} l
    full outer join {{ ref('int_license_latest_daily') }} s on s.id = l.id
