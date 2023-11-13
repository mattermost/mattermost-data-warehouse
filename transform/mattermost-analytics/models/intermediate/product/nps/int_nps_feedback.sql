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
    , feedback as feedback
    , user_role as user_role
    , received_at as feedback_received_at
    from {{ ref('stg_mattermost_nps__nps_feedback') }} 
    where feedback is not null
    qualify row_number() over (partition by user_id, feedback order by timestamp desc) = 1
), mm_plugin_prod as 
(
 select 
    server_id as server_id
    , user_id as user_id
    , license_id as license_id
    , event_date as event_date
    , timestamp as timestamp
    , server_version as server_version
    , feedback as feedback
    , user_role as user_role
    , received_at as feedback_received_at
    from {{ ref('stg_mm_plugin_prod__nps_feedback') }}
    where feedback is not null
    qualify row_number() over (partition by user_id, feedback order by timestamp desc) = 1
) 
select * from mattermost_nps
union
select * from mm_plugin_prod