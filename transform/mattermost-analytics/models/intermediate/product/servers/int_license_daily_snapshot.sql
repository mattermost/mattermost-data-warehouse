{{
    config({
        "materialized": "table",
    })
}}
select
    coalesce(s.daily_server_id, l.daily_server_id) as daily_server_id,
    coalesce(s.server_id, l.server_id) as server_id,
    coalesce(s.server_date, l.server_date) as snapshot_date,
    coalesce(s.customer_id, l.customer_id) as customer_id,
    coalesce(s.license_id, l.license_id) as license_id,
    coalesce(s.sku_short_name, l.sku_short_name) as sku_short_name,
    coalesce(s.issued_at, l.issued_at) as issued_at,
    coalesce(s.expire_at, l.expire_at) as expire_at,
    coalesce(s.users, l.users) as users
from
    {{ ref('int_license_telemetry_legacy_latest_daily') }} l
    full outer join {{ ref('int_license_telemetry_latest_daily') }} s on s.daily_server_id = l.daily_server_id
