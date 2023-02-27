{{
    config({
        "materialized": "incremental",
        "cluster_by": ['date_received_at'],
    })
}}

SELECT * FROM 
{{ ref('stg_mm_telemetry_prod__performance_events') }}

{% if is_incremental() %}
    WHERE date_received_at >= (SELECT MAX(date_received_at) FROM {{ this }}) 
    {% endif %}