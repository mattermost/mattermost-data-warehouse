{# Events that have been merged to the base table are deleted in the post hook #}

{{
    config({
        'materialized': 'incremental',
        'incremental_strategy': 'delete+insert',
        'unique_key': ['id'],
        'cluster_by': ['to_date(received_at)'],
        'on_schema_change': 'append_new_columns',
        'snowflake_warehouse': 'transform_l',
    })
}}

with time_thresholds as (
    -- Last date of the base table will be used if (a) it's the first run of the model or (b) the model is incremental
    -- but doesn't contain any rows.
    select max(received_at) as time_threshold from {{ source('rudder_support', 'base_events') }}
{% if is_incremental() %}
    -- If model is incremental, also consider the last date of the model itself. If there are rows, then received_at
    -- will be greater than the max value of received at of the base table.
    union all
    select max(received_at) as time_threshold from {{ this }}
{% endif %}
)
select
    {{ dbt_utils.star(from=source('mm_telemetry_prod', 'event')) }}
from
    {{ source('mm_telemetry_prod', 'event') }}
where
    -- Event received in the past two days
    received_at >= (select dateadd(day, -2, max(time_threshold)) from time_thresholds)
    -- Event has not been merged to the base table
    and id not in (select id from {{ source('rudder_support', 'base_events') }} where received_at >= (select dateadd(day, -2, max(time_threshold)) from time_thresholds))
