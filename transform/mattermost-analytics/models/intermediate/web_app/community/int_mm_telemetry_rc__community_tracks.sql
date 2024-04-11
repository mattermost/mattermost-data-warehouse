{{
    config({
        "materialized": "incremental",
        "cluster_by": ['received_at_date'],
        "incremental_strategy": "append",
        "snowflake_warehouse": "transform_l"
    })
}}

    SELECT
        event_id AS event_id,
        event_table AS event_table,
        event_name AS event_name,
        category AS category,
        event_type AS event_type,
        server_id AS server_id,
        user_id AS user_id,
        received_at AS received_at,
        timestamp AS timestamp,
        'stg_mm_telemetry_rc__tracks' AS _source_relation,
        received_at::date as received_at_date
     FROM
       {{ ref('stg_mm_telemetry_rc__tracks') }}
    WHERE server_id = '{{ var("community_server_id") }}'
{% if is_incremental() %}
    AND received_at > (SELECT MAX(received_at) FROM {{ this }})
{% endif %}