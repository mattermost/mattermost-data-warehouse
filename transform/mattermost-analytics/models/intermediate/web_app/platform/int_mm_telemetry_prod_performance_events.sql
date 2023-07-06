{{
    config({
        "materialized": "incremental",
        "cluster_by": ['received_at_date'],
        "snowflake_warehouse": "transform_l"
    })
}}

SELECT
    {{ dbt_utils.star(ref('stg_mm_telemetry_prod__performance_events')) }}
FROM
    {{ ref('stg_mm_telemetry_prod__performance_events') }}
WHERE
    id not in (
        'd\';waitfor/**/delay\'0:0:0\'/**/--/**/',
        's\');waitfor/**/delay\'0:0:0\'/**/--/**/'
    )
{% if is_incremental() %}
    and received_at > (SELECT MAX(received_at) FROM {{ this }})
{% endif %}