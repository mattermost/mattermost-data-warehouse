{{config({
    "materialized": 'incremental',
    "transient": true,
    "snowflake_warehouse": "transform_l"
  })
}}
select
    {{dbt_utils.star(source('mm_telemetry_prod', 'event') )}},
    current_timestamp() as watermark
from
    {{ source('mm_telemetry_prod', 'event') }}
where
{% if is_incremental() %}
    received_at >= (select max(received_at) FROM {{ this }})
{% else %}
    received_at >= '2024-10-01'
{% endif %}
