{{config({
    "materialized": 'incremental',
    "transient": true,
    "snowflake_warehouse": "transform_l",
    "incremental_strategy": "delete+insert",
    "unique_key": ['id'],
  })
}}
select
    {{dbt_utils.star(source('mm_telemetry_prod', 'event') )}}
from
    {{ source('mm_telemetry_prod', 'event') }}
where
{% if is_incremental() %}
    received_at >= (select max(received_at) FROM {{ this }})
{% else %}
    received_at >= '2024-09-01'
{% endif %}
