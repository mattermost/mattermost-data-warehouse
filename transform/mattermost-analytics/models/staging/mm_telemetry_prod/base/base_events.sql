{{config({
    "materialized": "incremental",
    "snowflake_warehouse": "transform_l",
  })
}}
select
    {{dbt_utils.star(source('mm_telemetry_prod', 'event') )}}
from
    {{ source('mm_telemetry_prod', 'event') }}
where
{% if is_incremental() %}
    -- Hack to disable incremental for now
    false
{% else %}
    timestamp < '2024-01-01'
{% endif %}
