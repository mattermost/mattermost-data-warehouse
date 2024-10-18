{# Keep events that have not been merged into the base_events table #}

{{
    config({
        'materialized': 'incremental',
        'incremental_strategy': 'delete+insert',
        'unique_key': ['id'],
        'cluster_by': ['to_date(received_at)'],
        'on_schema_change': 'append_new_columns',
        'snowflake_warehouse': 'transform_l',
        'post_hook': 'delete from {{this}} where received_at < (select max(received_at) from {{ source(\'rudder_support\', \'base_events\') }})'
    })
}}

select
    {{ dbt_utils.star(from=source('mm_telemetry_prod', 'event')) }}
from
    {{ source('mm_telemetry_prod', 'event') }}
where
{% if is_incremental() %}
    received_at >= (select max(received_at) from {{ this }})
{% else %}
    received_at >= (select max(received_at) from {{ source('rudder_support', 'base_events') }})
{% endif %}