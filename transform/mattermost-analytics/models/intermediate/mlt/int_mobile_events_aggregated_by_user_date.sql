{{
    config({
        "materialized": "incremental",
        "incremental_strategy": "merge",
        "unique_key": ['daily_event_id'],
        "merge_update_columns": ['event_count'],
        "cluster_by": ['received_at_date'],
    })
}}

select user_id,timestamp::date as event_date, count(*)  from {{ ref ('stg_mm_mobile_prod__tracks') }} group by 1,2