-- Materializing this intermediate table as it's used multiple times downstream.
{{
    config({
        "materialized": "table",
        "snowflake_warehouse": "transform_l"
    })
}}

with nps as (
select 
    distinct coalesce(f.server_id, s.server_id) server_id
    , coalesce(f.user_id, s.user_id) user_actual_id
    , coalesce(f.event_date, s.event_date) event_date
    , coalesce(f.server_version, s.server_version) server_version
    , f.feedback feedback
    , coalesce(f.user_role, s.user_role) user_role
    , coalesce(f.received_at, s.received_at) received_at
    , s.score score
    from {{ ref('stg_mm_plugin_prod__nps_feedback') }} f
        full join {{ ref('stg_mm_plugin_prod__nps_score') }} s
            on f.server_id = s.server_id 
            and f.user_id = s.user_id 
            and f.event_date = s.event_date
)
select * from nps
