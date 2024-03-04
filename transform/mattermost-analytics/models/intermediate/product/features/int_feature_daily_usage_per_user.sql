{{
    config({
        "materialized": "incremental",
        "incremental_strategy": "merge",
        "merge_update_columns": ['received_at_date', 'count'],
        "unique_key": ['_daily_user_event_key'],
        "cluster_by": ['received_at_date'],
        "snowflake_warehouse": "transform_l"
    })
}}


select
    cast(received_at as date) as received_at_date
    , cast(timestamp as date) as activity_date
    , server_id
    , user_id
    , event_name
    , category
    , event_type
    , {{
        dbt_utils.generate_surrogate_key([
            'received_at_date',
            'activity_date',
            'server_id',
            'user_id',
            'event_name',
            'category',
            'event_type'
        ])
    }} as _daily_user_event_key
    , count(*) as event_count
from
    {{ ref('stg_mm_telemetry_prod__tracks') }}
where
    -- Exclude items without user info
    user_id is not null
    -- Exclude items without server ids
    and server_id is not null
    -- Exclude items with missing timestamps
    and timestamp is not null
    -- Exclude items from the future
    and received_at <= current_timestamp
{% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    and received_at >= (select max(received_at_date) from {{ this }})
{% endif %}
group by received_at_date, activity_date, server_id, user_id, event_name, category, event_type

