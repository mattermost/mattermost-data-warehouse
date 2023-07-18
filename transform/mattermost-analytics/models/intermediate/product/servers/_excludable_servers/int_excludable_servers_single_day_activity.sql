{{config({
    "materialized": "table",
    "snowflake_warehouse": "transform_l",
  })
}}

select
    server_id,
    case
        when count_server_active_days = 1 and count_user_active_days = 0 then 'Single day server-side telemetry only'
        when count_server_active_days = 0 and count_user_active_days = 1 then 'Single day user telemetry only'
        when count_server_active_days = 1 and count_user_active_days = 1 then 'Single day telemetry only'
        else null
    end as reason
from
    {{ ref('_int_server_telemetry_summary') }}
where
    reason is not null
