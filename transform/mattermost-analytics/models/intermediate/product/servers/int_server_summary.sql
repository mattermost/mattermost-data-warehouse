-- Materialize as table in order to break chained view dependency.  This model performs quite a few operations,
-- so it's a good candidate to materialize to table rather than ephemeral.
-- See https://dbt-labs.github.io/dbt-project-evaluator/0.7/rules/performance/#chained-view-dependencies for more
-- details
{{
    config({
        "materialized": "table",
        "snowflake_warehouse": "transform_l"
    })
}}
with server_side_activity as (
    select
        distinct server_id, server_date
    from
        {{ ref('int_server_telemetry_legacy_latest_daily') }}
    union
    select
        distinct server_id, server_date
    from
        {{ ref('int_server_telemetry_latest_daily') }}
),
server_summary as (
    -- Servers side telemetry summary
    select
        server_id,
        count(distinct server_date) as count_server_active_days,
        min(server_date) as first_date,
        max(server_date) as last_date
    from
        server_side_activity
    group by 1
),
user_activity as (
    select
        distinct server_id, activity_date
    from
        {{ ref('int_user_active_days_legacy_telemetry') }}
    union
    select
        distinct server_id, activity_date
    from
        {{ ref('int_user_active_days_server_telemetry') }}
),
user_summary as (
    -- User telemetry summary
    select
        server_id,
        count(distinct activity_date) as count_user_active_days,
        min(activity_date) as first_date,
        max(activity_date) as last_date
    from
        user_activity
    group by 1
),
 diagnostics_summary as (
    select
        server_id,
        count(distinct log_date) as count_diagnostics_active_days,
        min(log_date) as first_date,
        max(log_date) as last_date
    from
        {{ ref('stg_diagnostics__log_entries') }}
    group by server_id
), all_server_ids as (
    select distinct server_id from server_summary
    union
    select distinct server_id from user_summary
    union
    select distinct server_id from diagnostics_summary
)
select
    s.server_id,
    coalesce(ss.count_server_active_days, 0) as count_server_active_days,
    coalesce(us.count_user_active_days, 0) as count_user_active_days,
    coalesce(ds.count_diagnostics_active_days, 0) as count_diagnostics_active_days,
    ss.first_date as server_telemetry_first_date,
    ss.last_date as server_telemetry_last_date,
    us.first_date as user_telemetry_first_date,
    us.last_date as user_telemetry_last_date,
    ds.first_date as diagnostics_telemetry_first_date,
    ds.last_date as diagnostics_telemetry_last_date
from
    all_server_ids s
    left join server_summary ss on s.server_id = ss.server_id
    left join user_summary us on s.server_id = us.server_id
    left join diagnostics_summary ds on s.server_id = ds.server_id