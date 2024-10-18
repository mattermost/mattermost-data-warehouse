{{
    config({
        'materialized': 'incremental',
        'incremental_strategy': 'delete+insert',
        'unique_key': ['id'],
        'cluster_by': ['received_at_date'],
        'on_schema_change': 'append_new_columns'
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