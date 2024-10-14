{{config({
    'materialized': 'table',
    'transient': true,
    'snowflake_warehouse': 'transform_l'
  })
}}
select
    {{dbt_utils.star(source('mm_telemetry_prod', 'event') )}}
from
    {{ source('mm_telemetry_prod', 'event') }}
where
    received_at >= (select max(received_at) FROM {{ ref('base_events') }})
qualify 1 = row_number() over (partition by id order by received_at desc)