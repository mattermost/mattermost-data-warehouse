with mattermost_nps as (
select 
    distinct server_id as server_id
    , user_id as user_id
    , event_date as event_date
    , timestamp as timestamp
    , server_version as server_version
    , score as score
    , user_role as user_role
    , received_at as score_received_at
    from {{ ref('stg_mattermost_nps__nps_score') }} 
), mm_plugin_prod as 
(
 select 
    distinct server_id as server_id
    , user_id as user_id
    , event_date as event_date
    , timestamp as timestamp
    , server_version as server_version
    , score as score
    , user_role as user_role
    , received_at as score_received_at
    from {{ ref('stg_mm_plugin_prod__nps_score') }}    
) 
select * from mattermost_nps
union
select * from mm_plugin_prod
