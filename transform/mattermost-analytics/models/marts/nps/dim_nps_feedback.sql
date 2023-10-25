select distinct nf.server_id as server_id
       , nf.user_id as user_id
       , nf.license_id as license_id
       , nf.feedback as feedback
       , ns.score as score
       , nf.server_version as server_version
       , nf.hosting_type as hosting_type
       , nf.user_role as user_role
       , nf.event_date feedback_date
       , ns.event_date score_date
       , fau.count_registered_users as registered_users
    from {{ ref('int_nps_feedback') }} nf 
    left join {{ ref('int_nps_score') }} ns 
        on nf.server_id = ns.server_id and nf.user_id = ns.user_id and nf.event_date = ns.event_date
    left join {{ ref('int_server_active_days_spined') }} fau 
        on nf.server_id = fau.server_id and nf.event_date = fau.activity_date
    where nf.feedback is not null
