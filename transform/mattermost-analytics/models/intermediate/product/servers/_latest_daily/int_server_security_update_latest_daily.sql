{{
    config({
        "materialized": "table",
        "snowflake_warehouse": "transform_l"
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
    server_ip,
    operating_system,
    database_type,
    is_enterprise_ready,
    -- Can be used to identify potential upgrade/upgrade attempts or erroneous data
    count(distinct version_full) over (partition by server_id, server_date) as count_reported_versions,
    array_unique_agg(version_full) over (partition by server_id, server_date) as reported_versions,
    count_teams,
    count_users,
    count_active_users
from
    {{ ref('stg_diagnostics__log_entries') }}
{% if is_incremental() %}
where
    -- this filter will only be applied on an incremental run
    log_date >= (select max(server_date) from {{ this }})
{% endif %}
-- Keep latest record per day
qualify row_number() over (partition by server_id, server_date order by log_at desc) = 1