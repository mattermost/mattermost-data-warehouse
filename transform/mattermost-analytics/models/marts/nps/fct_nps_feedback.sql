{{
    config({
        "materialized": "incremental",
        "cluster_by": ['feedback_date'],
        "incremental_strategy": "append"
    })
}}

select distinct nf.server_id as server_id
       , nf.user_id as user_id
       , nf.license_id as license_id
       , nf.feedback as feedback
       , ns.score as score
       , nf.server_version as server_version
       , coalesce(si.hosting_type, nf.hosting_type) as hosting_type
       , nf.user_role as user_role
       , COALESCE(cc.company_name, sh.company_name) as company_name
       , COALESCE(cc.customer_email, sh.customer_email) as customer_email
       , nf.event_date as feedback_date
       , ns.event_date as score_date
       , fau.count_registered_users as registered_users
       , nf.timestamp as timestamp
    from {{ ref('int_nps_feedback') }} nf 
    left join {{ ref('int_nps_score') }} ns 
        on nf.server_id = ns.server_id and nf.user_id = ns.user_id and nf.event_date = ns.event_date
    left join {{ ref('dim_server_info') }} si
        on nf.server_id = si.server_id 
    left join {{ ref('int_server_active_days_spined') }} fau 
        on nf.server_id = fau.server_id and nf.event_date = fau.activity_date
    left join {{ ref('int_cloud_customers') }} cc 
        on nf.server_id = cc.server_id 
    left join {{ ref('int_self_hosted_customers') }} sh
        on nf.server_id = sh.server_id or nf.license_id = sh.license_id
    where nf.feedback is not null 
{% if is_incremental() %}
    and timestamp > (SELECT MAX(timestamp) FROM {{ this }})
{% endif %}

