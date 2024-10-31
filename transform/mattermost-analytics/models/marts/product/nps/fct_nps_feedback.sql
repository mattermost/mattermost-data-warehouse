select {{ dbt_utils.generate_surrogate_key(['nf.server_id', 'nf.user_id', 'nf.feedback', 'nf.event_date']) }} as nps_feedback_id
       , {{ dbt_utils.generate_surrogate_key(['nf.server_id', 'nf.event_date']) }} as daily_server_id
       , nf.server_id as server_id
       , nf.user_id as user_id
       , nf.license_id as license_id
       , nf.feedback as feedback
       , ns.score as score
       , coalesce(nf.server_version_full, ns.server_version_full) as all_server_version_full
       , {{ dbt_utils.generate_surrogate_key(['all_server_version_full']) }} AS version_id 
       , nf.user_role as user_role
       , nf.event_date as feedback_date
       , ns.event_date as score_date
       , nf.timestamp as feedback_timestamp
       , nf.user_email as user_email
    from {{ ref('int_nps_feedback') }} nf 
    left join {{ ref('int_nps_score') }} ns 
        on nf.server_id = ns.server_id and nf.user_id = ns.user_id and nf.event_date = ns.event_date
    where nf.feedback is not null 

