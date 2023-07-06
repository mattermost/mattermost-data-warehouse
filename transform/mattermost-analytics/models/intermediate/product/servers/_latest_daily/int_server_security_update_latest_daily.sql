{{
    config({
        "materialized": "incremental",
        "incremental_strategy": "merge",
        "merge_update_columns": ['count_reported_versions', 'reported_versions'],
        "unique_key": ['daily_server_id'],
        "cluster_by": ['server_date'],
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
    array_unique_agg(version_full) over (partition by server_id, server_date) as reported_versions
from
    {{ ref('stg_diagnostics__log_entries') }}
where
    -- Exclude custom builds
    not is_custom_build_number
    -- Ignore test builds
    and not has_run_unit_tests
    -- Exclude servers from these IPs as they were used for a large number of tests
    and server_ip not in ('194.30.0.183', '194.30.0.184')
{% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    and log_date >= (select max(server_date) from {{ this }})
{% endif %}
-- Keep latest record per day
qualify row_number() over (partition by server_id, server_date order by log_at desc) = 1