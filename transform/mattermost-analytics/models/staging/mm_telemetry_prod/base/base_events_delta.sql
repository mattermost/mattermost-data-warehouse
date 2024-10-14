{{config({
    'materialized': 'table',
    'transient': true,
    'snowflake_warehouse': 'transform_l',
    'cluster_by': ['to_date(timestamp)'],
  })
}}
select
    {{dbt_utils.star(source('mm_telemetry_prod', 'event') )}}
from
    {{ source('mm_telemetry_prod', 'event') }}
where
    received_at >= (select max(received_at) FROM {{ reF('base_events') }})
