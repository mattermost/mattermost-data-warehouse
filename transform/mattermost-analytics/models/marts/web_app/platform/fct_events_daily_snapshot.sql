{{
    config({
        "cluster_by": ['event_date']
    })
}}

SELECT *
    {{ ref('int_performance_events') }}
