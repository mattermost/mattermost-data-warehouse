{{config({
    'materialized': 'incremental',
    'snowflake_warehouse': 'transform_l',
  })
}}
select
    {{dbt_utils.star(source('mm_telemetry_prod', 'event') )}}
from
    {{ source('mm_telemetry_prod', 'event') }}
where
{% if is_incremental() %}
    -- Hack to run only once
    false
{% else %}
    received_at < current_date()
{% endif %}
