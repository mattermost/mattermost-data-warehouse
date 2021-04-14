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
        receive_emails_accepted,
        trial_requests.sfid as trial_requests_sfid,
        max(license_issued_at) as license_issued_at
    from {{ ref('trial_requests') }}
    where trial_requests.site_url != 'https://mattermost.com'
        and REGEXP_LIKE(UPPER(email),'^[A-Z0-9._%+-/!#$%&''*=?^_`{|}~]+@[A-Z0-9.-]+\\.[A-Z]{2,4}$')
    {{ dbt_utils.group_by(n=10) }}
), contact_in_product_trial_request as (
    select 
        recent_in_product_trial_requests.email,
        recent_in_product_trial_requests.first_name,
        recent_in_product_trial_requests.last_name,
        recent_in_product_trial_requests.ownerid,
        recent_in_product_trial_requests.most_recent_action,
        recent_in_product_trial_requests.most_recent_action_detail,
        recent_in_product_trial_requests.most_recent_lead_source,
        recent_in_product_trial_requests.most_recent_lead_source_detail,
        not recent_in_product_trial_requests.receive_emails_accepted as optedoutofemail,
        recent_in_product_trial_requests.license_issued_at,
        '7013p000001NkNoAAK' as in_product_trial_request_campaignid,
        'Responded' as campaign_status,
        min(contact.sfid) as contact_sfid
    from recent_in_product_trial_requests
    join {{ ref('contact') }} on lower(contact.email) = recent_in_product_trial_requests.email or recent_in_product_trial_requests.trial_requests_sfid = contact.sfid 
    where (contact.trial_req_date__c is null or contact.trial_req_date__c::date < license_issued_at::date)
    {{ dbt_utils.group_by(n=11) }}
)

select * from contact_in_product_trial_request