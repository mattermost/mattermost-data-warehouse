{{
    config({
        "materialized": "incremental",
        "cluster_by": ['received_at_date'],
    })
}}

SELECT * FROM 
{{ ref('stg_mm_telemetry_prod__performance_events') }}

{% if is_incremental() %}
    WHERE received_at_date >= (SELECT MAX(received_at_date) FROM {{ this }}) 
    {% endif %}