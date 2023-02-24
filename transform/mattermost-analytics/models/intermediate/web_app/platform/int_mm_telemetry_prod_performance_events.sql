{{
    config({
        "materialized": "incremental",
        "cluster_by": ['event_date'],
    })
}}

SELECT * FROM 
{{ ref('stg_mm_telemetry_prod__performance_events') }}

{% if is_incremental() %}
    WHERE event_date >= (SELECT MAX(event_date) FROM {{ this }}) 
    {% endif %}