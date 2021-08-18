{{
    config({
    "materialized": 'table',
    "schema": "hightouch"
    })
}}


with recent_in_product_trial_requests as (
    select 
        lower(email) as email,
        case when ARRAY_SIZE(split(name,' ')) > 1 then split_part(name,' ',1) else name end as first_name,
        case when ARRAY_SIZE(split(name,' ')) > 1 then split_part(name,' ',2) else email end as last_name,
        '0051R00000GnvhhQAB' as ownerid,
        'Enterprise Trial' as most_recent_action,
        'In-Product Trial Request' as most_recent_action_detail,
        'Referral' as most_recent_lead_source,
        'MM Product' as most_recent_lead_source_detail,
        case when site_name = 'Mattermost' then 'Unknown' else site_name end as company,
        receive_emails_accepted,
        trial_requests.sfid as trial_requests_sfid,
        max(license_issued_at) as license_issued_at
    from {{ ref('trial_requests') }}
    where not exists (select 1 from {{ ref('contact') }} where lower(contact.email) = lower(trial_requests.email) or trial_requests.sfid = contact.sfid)
        and trial_requests.site_url != 'https://mattermost.com'
        and REGEXP_LIKE(UPPER(email),'^[A-Z0-9._%+-/!#$%&''*=?^_`{|}~]+@[A-Z0-9.-]+\\.[A-Z]{2,4}$')
    {{ dbt_utils.group_by(n=11) }}
), lead_in_product_trial_request as (
    select 
        recent_in_product_trial_requests.email,
        coalesce(lead.firstname,recent_in_product_trial_requests.first_name) as first_name,
        coalesce(lead.firstname,recent_in_product_trial_requests.last_name) as last_name,
        recent_in_product_trial_requests.most_recent_action,
        recent_in_product_trial_requests.most_recent_action_detail,
        recent_in_product_trial_requests.most_recent_lead_source,
        recent_in_product_trial_requests.most_recent_lead_source_detail,
        not recent_in_product_trial_requests.receive_emails_accepted as optedoutofemail,
        '7013p000001NkNoAAK' as in_product_trial_request_campaignid,
        'Responded' as campaign_status,
        coalesce(lead.dwh_external_id__c, uuid_string()) as lead_dwh_external_id,
        lead.sfid as lead_sfid,
        campaignmember.sfid as campaignmember_sfid,
        coalesce(campaignmember.dwh_external_id__c, uuid_string()) as campaignmember_dwh_external_id,
        max(coalesce(lead.company,recent_in_product_trial_requests.company)) as company,
        max(recent_in_product_trial_requests.license_issued_at) as license_issued_at
    from recent_in_product_trial_requests
    left join {{ ref('lead') }} on lower(lead.email) = recent_in_product_trial_requests.email or recent_in_product_trial_requests.trial_requests_sfid = lead.sfid 
    left join {{ ref('campaignmember') }} on lead.sfid = campaignmember.leadid and campaignmember.campaignid = '7013p000001NkNoAAK'
    where (lead.request_a_trial_date__c is null or lead.request_a_trial_date__c::date < license_issued_at::date)
    {{ dbt_utils.group_by(n=14) }}
)

select * from lead_in_product_trial_request