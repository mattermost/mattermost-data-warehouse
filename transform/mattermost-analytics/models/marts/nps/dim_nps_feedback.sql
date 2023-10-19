-- Materializing this intermediate table for test purposes.
{{
    config({
        "snowflake_warehouse": "transform_l"
    })
}}

select nf.feedback as feedback
       , ns.score as score
       , nf.server_version as server_version
       , si.installation_type as installation_type
       , nf.user_role as user_role
       , nf.event_date feedback_date
       , ns.event_date score_date
    from {{ ref('int_nps_feedback') }} nf 
    left join {{ ref('int_nps_score') }} ns 
        on nf.server_id = ns.server_id and nf.user_id = ns.user_id and nf.event_date = ns.event_date
    left join {{ ref('dim_server_info') }} si 
        on nf.server_id = si.server_id
