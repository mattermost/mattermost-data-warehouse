{{
    config({
        "materialized": "incremental",
        "cluster_by": ['received_at_date'],
        "incremental_strategy": "delete+insert",
        "unique_key": ['event_id'],
        "snowflake_warehouse": "transform_l",
    })
}}

SELECT
    {{ dbt_utils.star(ref('stg_mm_telemetry_rc__tracks')) }},
    'stg_mm_telemetry_rc__tracks' AS _source_relation,
    received_at::date as received_at_date
FROM
   {{ ref('stg_mm_telemetry_rc__tracks') }}
WHERE server_id = '{{ var("community_server_id") }}'
{% if is_incremental() %}
    AND received_at >= (SELECT MAX(received_at) FROM {{ this }})
{% endif %}
-- Dedup events in the same batch
qualify row_number() over (partition by event_id order by timestamp desc) = 1