{{
    config({
        "materialized": "incremental",
        "unique_key": ['event_id'],
        "incremental_strategy": "delete+insert",
        "cluster_by": ['received_at_date'],
        "snowflake_warehouse": "transform_l"
    })
}}

select 
     {{ dbt_utils.star(ref('stg_mm_telemetry_prod__oauth')) }}
     , cast(received_at AS date) AS received_at_date
     , true as has_rudderstack_telemetry_data
     , false as has_segment_telemetry_data
    from {{ ref('stg_mm_telemetry_prod__oauth') }} tmp
    where
    received_at <= CURRENT_TIMESTAMP
{% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    and received_at >= (SELECT max(received_at) FROM {{ this }})
{% endif %}
