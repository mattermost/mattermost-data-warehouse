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
    where not exists (select 1 from {{ ref('contact') }} where lower(contact.email) = lower(trial_requests.email) or trial_requests.sfid = contact.sfid)
        and license_issued_at > current_date - interval '1 day'
    {{ dbt_utils.group_by(n=10) }}
), map_to_leads as (
    select recent_in_product_trial_requests.*,
    request_a_trial_date__c,
    lead.sfid as lead_sfid,
    coalesce(min(lead.dwh_external_id__c), uuid_string()) as dwh_external_id
    from recent_in_product_trial_requests
    left join {{ ref('lead') }} on lower(lead.email) = recent_in_product_trial_requests.email or recent_in_product_trial_requests.trial_requests_sfid = lead.sfid 
    where (lead.request_a_trial_date__c is null or lead.request_a_trial_date__c::date < license_issued_at::date)
    {{ dbt_utils.group_by(n=13) }}
)
select * from map_to_leads