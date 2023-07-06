select
    server_id,
    log_date AS server_date,
    {{ dbt_utils.generate_surrogate_key(['server_id', 'server_date']) }} AS daily_server_id,
    version_full,
    version_major,
    version_minor,
    version_patch,
    cip as server_ip,
    operating_system,
    database_type,
    is_enterprise_ready
from
    {{ ref('stg_diagnostics__log_entries') }}
-- Keep latest record per day
qualify row_number() over (partition by server_id, server_date order by timestamp desc) = 1