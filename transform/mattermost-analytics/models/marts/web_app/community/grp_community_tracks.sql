{{
    config({
        "materialized": "incremental",
        "cluster_by": ['event_date'],
        "incremental_strategy": "append",
        "snowflake_warehouse": "transform_l"
    })
}}

WITH community_prod AS (
    SELECT 
        'stg_mm_telemetry_prod__tracks' AS _source_relation,
        {{dbt_utils.star(ref('stg_mm_telemetry_prod__tracks'))}}
        , timestamp::date as event_date
     FROM
       {{ ref('stg_mm_telemetry_prod__tracks') }} 
    WHERE server_id = '{{ var("community_server_id") }}'
{% if is_incremental() %}
    AND received_at > (SELECT MAX(received_at) FROM {{ this }}
    WHERE _source_relation = 'stg_mm_telemetry_prod__tracks') 
{% endif %}
), 
community_rc AS (
    SELECT 
        'stg_mm_telemetry_rc__tracks' AS _source_relation,
        {{dbt_utils.star(ref('stg_mm_telemetry_rc__tracks'))}}
        , timestamp::date as event_date
     FROM
       {{ ref('stg_mm_telemetry_rc__tracks') }} 
    WHERE server_id = '{{ var("community_server_id") }}'
{% if is_incremental() %}
    AND received_at > (SELECT MAX(received_at) FROM {{ this }}   
    WHERE _source_relation = 'stg_mm_telemetry_rc__tracks') 
{% endif %}
)
  SELECT * FROM community_prod
  UNION ALL
  SELECT * FROM community_rc