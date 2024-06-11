{{
    config({
        "materialized": "incremental",
        "incremental_strategy": "delete+insert",
        "unique_key": ['daily_user_id'],
        "cluster_by": ['received_at_date'],
        "snowflake_warehouse": "transform_l"
    })
}}

select
    cast(received_at as date) as received_at_date
    , cast(timestamp as date) as activity_date
    , server_id
    , user_id
    {{ dbt_utils.generate_surrogate_key(['received_at_date', 'activity_date', 'server_id', 'user_id']) }} as daily_user_id
    , count_if(tp.feature_name = 'playbooks') as count_playbooks
    , count_playbooks as count_known_feature
    , count_if(tp.feature_name is null) as count_unknown_feature
    , count(event_id) as count_total_events
from
    {{ ref('stg_incident_response_prod__tracks') }} e
    left join {{ ref('playbooks_tracking_plan' ) }} tp on e.event_name = tp.event_name
        -- Handle nulls in event_action
        and ((e.event_action = tp.event_action) or (e.event_action is null and tp.event_action is null))
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
group by
    daily_user_id
    , activity_date
    , server_id
    , user_id
    , received_at_date
