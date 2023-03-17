{{
    config({
        "materialized": "incremental",
        "incremental_strategy": "merge",
        "unique_key": ['daily_event_id'],
        "merge_update_columns": ['event_count'],
        "cluster_by": ['received_at_date'],
    })
}}

{{
    rudder_daily_event_count(
        ref('stg_portal_prod__tracks'),
        by_columns=['event_table', 'event_name', 'category'],
        source_name='Portal Prod'
    )
}}
