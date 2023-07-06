{{
    config({
        "materialized": "table"
    })
}}
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
    is_enterprise_ready,
    -- Can be used to identify potential upgrade/upgrade attempts or erroneous data
    count(distinct version_full) over (partition by server_id, server_date) as count_reported_versions,
    array_unique_agg(version_full) over (partition by server_id, server_date) as reported_versions
from
    {{ ref('stg_diagnostics__log_entries') }}
-- Keep latest record per day
qualify row_number() over (partition by server_id, server_date order by log_at desc) = 1