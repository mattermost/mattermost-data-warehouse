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
    , count(distinct case when tp.feature_name = 'playbooks' then event_id end) as count_playbooks
    , count_playbooks as count_known_features
    , count(distinct case when tp.feature_name is null then event_id end) as count_unknown_features
    , count(distinct event_id) as count_total
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
group by
    daily_user_id
    , activity_date
    , server_id
    , user_id