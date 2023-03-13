{{
    config({
        "materialized": "incremental",
        "cluster_by": ['received_at_date'],
        "snowflake_warehouse": "transform_l"
    })
}}

SELECT * FROM 
{{ ref('stg_mm_telemetry_prod__performance_events') }}

{% if is_incremental() %}
    WHERE received_at > (SELECT MAX(received_at) FROM {{ this }}) 
    {% endif %}