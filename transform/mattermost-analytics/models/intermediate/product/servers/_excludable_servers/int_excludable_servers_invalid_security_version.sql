with version_exclusions as (
    select
        server_id,
        case when server_ip = '194.30.0.184' then 'Restricted IP' end as restricted_ip,
        case when has_run_unit_tests then 'Ran Tests' end as ran_tests,
        case when count_users < count_active_users then 'Active Users > Registered Users' end as user_count_sanity_check
        -- One check in existing logic does not work:
        -- dev_build is never 1
    from {{ ref('stg_diagnostics__log_entries') }}
)
select
    server_id, reason
from
    version_exclusions
    unpivot(reason for explanation in (restricted_ip, ran_tests, user_count_sanity_check))
where
    reason is not null