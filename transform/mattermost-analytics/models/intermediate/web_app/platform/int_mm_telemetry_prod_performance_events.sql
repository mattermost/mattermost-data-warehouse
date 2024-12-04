{{
    config({
        "materialized": "incremental",
        "cluster_by": ['received_at_date'],
        "incremental_strategy": "delete+insert",
        "unique_key": ['id'],
        "snowflake_warehouse": "transform_l"
    })
}}

SELECT
    {{ dbt_utils.star(ref('stg_mm_telemetry_prod__performance_events')) }}
FROM
    {{ ref('stg_mm_telemetry_prod__performance_events') }}
WHERE
    -- Exclude non-UUID strings
    not id ilike '%waitfor%'
{% if is_incremental() %}
    and received_at >= (SELECT MAX(received_at) FROM {{ this }})
{% endif %}