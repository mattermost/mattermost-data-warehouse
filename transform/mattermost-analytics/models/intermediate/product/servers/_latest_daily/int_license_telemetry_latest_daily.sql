select
    server_id,
    CAST(timestamp AS date) AS server_date,
    {{ dbt_utils.generate_surrogate_key(['server_id', 'server_date']) }} AS daily_server_id,
    customer_id,
    license_id,
    sku_short_name,
    issued_at,
    expire_at,
    users
from
    {{ ref('stg_mm_telemetry_prod__license') }}
-- Keep latest record per day
qualify row_number() over (partition by server_id, server_date order by timestamp desc) = 1