with security_exclusion_reasons as (
    select
        server_id,
        case when server_ip = '194.30.0.184' then 'Restricted IP' end as restricted_ip,
        case when has_run_unit_tests then 'Ran Tests' end as ran_tests,
        case when count_users < count_active_users then 'Active Users > Registered Users' end as user_count_sanity_check,
        case when is_custom_build_version_format then 'Custom Build Version Format' end as custom_build_version_format
        -- One check in existing logic does not work:
        -- dev_build is never 1
    from {{ ref('stg_diagnostics__log_entries') }}
)
select
    distinct server_id, reason
from
    security_exclusion_reasons
    unpivot(reason for explanation in (restricted_ip, ran_tests, user_count_sanity_check, custom_build_version_format))
where
    reason is not null