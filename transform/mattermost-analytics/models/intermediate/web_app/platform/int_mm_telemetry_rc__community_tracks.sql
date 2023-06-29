{{
    config({
        "materialized": "incremental",
        "cluster_by": ['received_at_date'],
        "incremental_strategy": "append"
        "snowflake_warehouse": "transform_l"
    })
}}

    SELECT
        {{dbt_utils.star(ref('stg_mm_telemetry_prod__tracks'))}},
        _received_at::date as received_at_date
     FROM
       {{ ref('stg_mm_telemetry_prod__tracks') }}
    WHERE server_id = '{{ var("community_server_id") }}'
{% if is_incremental() %}
    AND received_at > (SELECT MAX(_received_at) FROM {{ this }})
{% endif %}