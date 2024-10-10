with mattermost_nps as (
select 
    server_id as server_id
    , user_id as user_id
    , license_id as license_id
    , event_date as event_date
    , timestamp as timestamp
    , server_version_full
    , feedback as feedback
    , user_role as user_role
    , received_at as feedback_received_at
    , null as user_email
    from {{ ref('stg_mattermost_nps__nps_feedback') }} 
    where feedback is not null
    qualify row_number() over (partition by server_id, user_id, feedback order by timestamp desc) = 1
), mm_plugin_prod as 
(
 select 
    server_id as server_id
    , user_id as user_id
    , license_id as license_id
    , event_date as event_date
    , timestamp as timestamp
    , server_version_full
    , feedback as feedback
    , user_role as user_role
    , received_at as feedback_received_at
    , user_email as user_email
    from {{ ref('stg_mm_plugin_prod__nps_feedback') }}
    where feedback is not null
    qualify row_number() over (partition by server_id, user_id, feedback order by timestamp desc) = 1
) 
select * from mattermost_nps
union
select * from mm_plugin_prod