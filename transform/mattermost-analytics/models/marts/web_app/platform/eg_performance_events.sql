{{
    config({
        "cluster_by": ['event_date']
    })
}}

WITH performance_rc AS (
    SELECT 
        'int_mm_telemetry_rc__performance_events' AS _source_relation,
        {{dbt_utils.star(ref('int_mm_telemetry_rc__performance_events'))}}
     FROM
       {{ ref('int_mm_telemetry_rc__performance_events') }}
{% if is_incremental() %}
    WHERE received_at_date >= (SELECT MAX(received_at_date) FROM {{ this }}) 
    WHERE _source_relation = 'int_mm_telemetry_rc__performance_events'
{% endif %}
), performance_prod AS (
    SELECT
        'int_mm_telemetry_prod__performance_events' AS _source_relation,
        {{dbt_utils.star(ref('int_mm_telemetry_prod__performance_events'))}}
     FROM
       {{ ref('int_mm_telemetry_prod__performance_events') }}
{% if is_incremental() %}
    WHERE received_at_date >= (SELECT MAX(received_at_date) FROM {{ this }}) 
    WHERE _source_relation = 'int_mm_telemetry_prod__performance_events'
)
  SELECT * FROM performance_rc
  UNION_ALL
  SELECT * FROM performance_prod