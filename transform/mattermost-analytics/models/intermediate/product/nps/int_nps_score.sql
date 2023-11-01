-- Temporarily materialize during development
{{
    config({
        "materialized": "table",
        "snowflake_warehouse": "transform_l"
    })
}}

with mattermost_nps as (
select 
    server_id as server_id
    , user_id as user_id
    , license_id as license_id
    , event_date as event_date
    , timestamp as timestamp
    , server_version as server_version
    , score as score
    , user_role as user_role
    , received_at as score_received_at
    from {{ ref('stg_mattermost_nps__nps_score') }} 
    qualify ROW_NUMBER() over (PARTITION BY event_date, user_id ORDER BY timestamp DESC) = 1
), mm_plugin_prod as 
(
 select 
    server_id as server_id
    , user_id as user_id
    , license_id as license_id
    , event_date as event_date
    , timestamp as timestamp
    , server_version as server_version
    , score as score
    , user_role as user_role
    , received_at as score_received_at
    from {{ ref('stg_mm_plugin_prod__nps_score') }}    
    qualify ROW_NUMBER() over (PARTITION BY event_date, user_id ORDER BY timestamp DESC) = 1
) 
select server_id
        , user_id
        , license_id
        , event_date
        , timestamp
        , server_version
        , score
        , user_role
        , score_received_at 
from mattermost_nps
union
select server_id
        , user_id
        , license_id
        , event_date
        , timestamp
        , server_version
        , score
        , user_role
        , score_received_at  
from mm_plugin_prod
