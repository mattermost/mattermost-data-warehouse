with cloud_trial_requests as (
    select 
    email,
    cws_installation
    from 
    {{ ref('int_cloud_trial_requests') }}
    -- Fetch the most recent cloud trial
    qualify row_number() over (partition by email order by trial_start_at desc) = 1
)
select
    ctr.email,
    l.lead_id is not null as is_existing_lead,
    cm.campaign_member_id is not null as is_existing_campaign_member,
    -- Campaign member status
    CASE 
        WHEN ctr.cws is not null then 'Workspace Created'
        ELSE 'Email Verified'
    END as campaign_member_status,
    -- Extra validation
    {{ validate_email('tr.normalized_email') }} as is_valid_email
from
    cloud_trial_requests ctr
    left join {{ ref('stg_salesforce__lead') }} l on ctr.email = l.email
    left join {{ ref('stg_salesforce__campaign_member') }} cm 
        on l.lead_id = cm.lead_id and tr.normalized_email = cm.email and cm.campaign_id = '{{ var('cloud_enterprise_trial_campaign_id') }}'
    where 
    is_valid_email
