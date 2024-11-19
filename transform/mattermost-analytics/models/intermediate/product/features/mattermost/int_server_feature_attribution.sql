{{
    config({
        "materialized": "incremental",
        "incremental_strategy": "delete+insert",
        "unique_key": ['event_id'],
        "snowflake_warehouse": "transform_l"
    })
}}

select
    cast(received_at as date) as received_at_date
    , cast(timestamp as date) as activity_date
    , server_id
    , user_id
    , event_id
    , event_name
    , feature_name
from
    {{ ref('stg_mm_telemetry_prod__server_tracks') }}
where
    -- Exclude items without user info
    user_id is not null
    -- Exclude items without server ids
    and server_id is not null
    -- Exclude items with missing timestamps
    and timestamp is not null
    -- Exclude items before the first date of known data. This is an optimization to prune the dataset,
    -- especially when performing a full refresh.
    and received_at >= '2024-10-31'
    -- Exclude items from the future
    and received_at <= current_timestamp
{% if is_incremental() %}
    -- This filter will only be applied on an incremental run
    -- TODO: handle late arriving events
    and received_at >= (select max(received_at_date) from {{ this }})
{% endif %}