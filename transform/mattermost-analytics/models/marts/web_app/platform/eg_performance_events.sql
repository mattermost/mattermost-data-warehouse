{{
    config({
        "materialized": "incremental",
        "cluster_by": ['event_date']
    })
}}

WITH performance_rc AS (
    SELECT 
        'int_mm_telemetry_rc_performance_events' AS _source_relation,
        {{dbt_utils.star(ref('int_mm_telemetry_rc_performance_events'))}}
     FROM
       {{ ref('int_mm_telemetry_rc_performance_events') }}
{% if is_incremental() %}
    WHERE received_at_date >= (SELECT MAX(received_at_date) FROM {{ this }}) 
    AND _source_relation = 'int_mm_telemetry_rc_performance_events'
{% endif %}
), 
performance_prod AS (
    SELECT
        'int_mm_telemetry_prod_performance_events' AS _source_relation,
        {{dbt_utils.star(ref('int_mm_telemetry_prod_performance_events'))}}
     FROM
       {{ ref('int_mm_telemetry_prod_performance_events') }}
{% if is_incremental() %}
    WHERE received_at_date >= (SELECT MAX(received_at_date) FROM {{ this }}) 
    AND _source_relation = 'int_mm_telemetry_prod_performance_events'
{% endif %}
)
  SELECT * FROM performance_rc
  UNION_ALL
  SELECT * FROM performance_prod