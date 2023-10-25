with mattermost_nps as (
select 
    distinct server_id as server_id
    , user_id as user_id
    , license_id as license_id
    , event_date as event_date
    , timestamp as timestamp
    , server_version as server_version
    , feedback as feedback
    , user_role as user_role
    , received_at as feedback_received_at
    , CASE WHEN license_id = '{{ var('cloud_license_id') }}' THEN 'Cloud' ELSE 'Self-Hosted' END hosting_type
    from {{ ref('stg_mattermost_nps__nps_feedback') }} 
), mm_plugin_prod as 
(
 select 
    distinct server_id as server_id
    , user_id as user_id
    , license_id as license_id
    , event_date as event_date
    , timestamp as timestamp
    , server_version as server_version
    , feedback as feedback
    , user_role as user_role
    , received_at as feedback_received_at
    , CASE WHEN license_id = '{{ var('cloud_license_id') }}' THEN 'Cloud' ELSE 'Self-Hosted' END hosting_type
    from {{ ref('stg_mm_plugin_prod__nps_feedback') }}    
) 
select * from mattermost_nps
union
select * from mm_plugin_prod
