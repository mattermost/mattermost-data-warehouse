{{
    config({
        "materialized": "incremental",
        "cluster_by": ['event_date']
    })
}}

WITH community_prod AS (
    SELECT 
        'stg_mm_telemetry_prod__tracks' AS _source_relation,
        {{dbt_utils.star(ref('stg_mm_telemetry_prod__tracks'))}}
        , received_at::date as received_at_date
        , timestamp::date as event_date
     FROM
       {{ ref('stg_mm_telemetry_prod__tracks') }} 
    WHERE server_id = '{{ var("community_server_id") }}'
{% if is_incremental() %}
    AND received_at_date >= (SELECT MAX(received_at_date) FROM {{ this }}) 
    AND _source_relation = 'stg_mm_telemetry_prod__tracks'
{% endif %}
), 
community_rc AS (
    SELECT 
        'stg_mm_telemetry_rc__tracks' AS _source_relation,
        {{dbt_utils.star(ref('stg_mm_telemetry_rc__tracks'))}}
        , received_at::date as received_at_date
        , timestamp::date as event_date
     FROM
       {{ ref('stg_mm_telemetry_rc__tracks') }} 
    WHERE server_id = '{{ var("community_server_id") }}'
{% if is_incremental() %}
    AND received_at_date >= (SELECT MAX(received_at_date) FROM {{ this }}) 
    AND _source_relation = 'stg_mm_telemetry_rc__tracks'
{% endif %}
)
  SELECT * FROM community_prod
  UNION_ALL
  SELECT * FROM community_rc