with cloud_trial_requests_pre as (
    select
        email,
        cws_installation,
        cws_dns,
        trial_start_at,
        trial_end_at
    from
        {{ ref('int_cloud_trial_requests') }} -- Fetch the most recent cloud trial
        qualify row_number() over (
            partition by email
            order by
                trial_start_at desc
        ) = 1
),
cloud_trial_requests as (
    select
        ctr.email,
        ctr.trial_start_at,
        ctr.trial_end_at,
        ctr.cws_installation,
        ctr.cws_dns,
        l.lead_id as existing_lead_id,
        cm.campaign_member_id as existing_campaign_member_id,
        '{{ var('cloud_enterprise_trial_campaign_id') }}' as campaign_id,
        l.lead_id is not null as is_existing_lead,
        cm.campaign_member_id is not null as is_existing_campaign_member,
        -- Campaign member status
        CASE
            WHEN ctr.cws_installation is not null then 'Workspace Created'
            ELSE null
        END as campaign_member_status,
        -- Extra validation
        {{ validate_email('ctr.email') }} as is_valid_email,
        {{ is_blacklisted_email('ctr.email') }} as is_blacklisted_email
    from
        cloud_trial_requests_pre ctr
        left join {{ ref('stg_salesforce__lead') }} l on ctr.email = l.email
        left join {{ ref('stg_salesforce__campaign_member') }} cm on l.lead_id = cm.lead_id
        and ctr.email = cm.email
        and cm.campaign_id = '{{ var('cloud_enterprise_trial_campaign_id') }}'
    where
        is_valid_email
        -- Rows may fan out in case of multiple leads with same email address, fetching the one with the latest created_at date.
        qualify row_number() over (
            partition by ctr.email
            order by
                l.created_at asc
        ) = 1
)
select
    *
from
    cloud_trial_requests