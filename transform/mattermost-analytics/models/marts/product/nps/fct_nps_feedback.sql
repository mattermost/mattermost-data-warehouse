select nf.server_id as server_id
       , nf.user_id as user_id
       , nf.license_id as license_id
       , nf.feedback as feedback
       , ns.score as score
       , nf.server_version as server_version
       , nf.user_role as user_role
       , nf.event_date as feedback_date
       , ns.event_date as score_date
       , nf.timestamp as timestamp
    from {{ ref('int_nps_feedback') }} nf 
    left join {{ ref('int_nps_score') }} ns 
        on nf.server_id = ns.server_id and nf.user_id = ns.user_id and nf.event_date = ns.event_date
    where nf.feedback is not null 

