{{
    config({
        "materialized": "incremental",
        "incremental_strategy": "merge",
        "cluster_by": ['timestamp'],
    })
}}

SELECT * FROM 
{{ ref('stg_mm_telemetry_rc__performance_events') }}