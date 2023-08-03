{{config({
    "materialized": "table",
    "snowflake_warehouse": "transform_l",
  })
}}
    --
-- List of all known server ids
--

with all_server_ids as (
    -- Server side telemetry
    select
        distinct server_id
    from
        {{ ref('int_server_telemetry_legacy_latest_daily') }}
    union
    select
        distinct server_id
    from
        {{ ref('int_server_telemetry_latest_daily') }}
    union
    -- User telemetry
    select
        distinct server_id
    from
        {{ ref('int_user_active_days_legacy_telemetry') }}
    union
    select
        distinct server_id
    from
        {{ ref('int_user_active_days_server_telemetry') }}
    -- Diagnostics
    select
        distict server_id
    from
        {{ ref('stg_diagnostics__log_entries') }}
)
select * from all_server_ids