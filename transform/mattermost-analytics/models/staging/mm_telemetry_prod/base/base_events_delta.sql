{{config({
    "materialized": 'incremental',
    "transient": true,
    "snowflake_warehouse": "transform_l",
    "incremental_strategy": "delete+insert",
    "unique_key": ['id'],
    "cluster_by": ['received_at_date'],
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
qualify 1 = row_number() over (partition by id order by received_at desc)
