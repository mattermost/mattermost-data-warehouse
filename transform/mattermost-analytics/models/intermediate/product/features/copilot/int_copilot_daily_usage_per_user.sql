{{
    config({
        "materialized": "table",
        "snowflake_warehouse": "transform_l"
    })
}}

select
    cast(timestamp as date) as activity_date
    , server_id
    , user_id
    , {{ dbt_utils.generate_surrogate_key(['activity_date', 'server_id', 'user_id']) }} as daily_user_id
    , count_if(feature_name = 'Copilot' and array_size(feature_skus) > 0) as count_copilot
    , count_copilot as count_known_features
    , count_if(array_size(feature_skus) = 0) as count_unknown_features
    , count(event_id) as count_total
from
    {{ ref('stg_copilot__tracks') }} e
where
    -- Exclude items without user info
    user_id is not null
    -- Exclude items without server ids
    and server_id is not null
    -- Exclude items with missing timestamps
    and timestamp is not null
    -- Exclude items from the future
    and received_at <= current_timestamp
group by
    daily_user_id
    , activity_date
    , server_id
    , user_id