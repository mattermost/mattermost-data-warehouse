{{
    config({
        "materialized": "incremental",
        "incremental_strategy": "merge",
        "merge_update_columns": ['received_at_date', 'event_count'],
        "unique_key": ['_daily_user_event_key'],
        "cluster_by": ['received_at_date'],
        "snowflake_warehouse": "transform_l"
    })
}}

with feature_aliases as (
    -- Get feature alias for each feature. This is required to get each feature as a column.
    select
        f.event_name
        , f.category
        , f.event_type
        , a.alias as feature_name
    from
        {{ ref('event_to_feature_mapping') }} f
        join {{ ref('feature_aliases') }} a on f.feature_name = a.feature_name
), aggregated_events as (
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
)
select
    e.received_at_date
    , e.activity_date
    , e.server_id
    , e.user_id
    , e.event_name
    , e.category
    , e.event_type
    , e._daily_user_event_key
    , e.event_count
    -- Mark known features and use a bucket for the rest
    , coalesce(f.feature_alias, 'unknown') as feature_alias
from
    aggregated_events e
    left join feature_aliases f on e.event_name = e.event_name and e.category = f.category and e.event_type = f.event_type

