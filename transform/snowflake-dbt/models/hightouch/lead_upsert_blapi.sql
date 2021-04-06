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
        max(license_issued_at) as license_issued_at
    from {{ ref('trial_requests') }}
    where license_issued_at > current_date - interval '1 day' and contact.sfid is null
    {{ dbt_utils.group_by(n=9) }}
), map_to_leads as (
    select recent_in_product_trial_requests.*,
    coalesce(min(lead.dwh_external_id__c), uuid_string())
    from recent_in_product_trial_requests
    left join {{ ref('contact') }} on lower(contact.email) = recent_in_product_trial_requests.email
    left join {{ ref('lead') }} on lower(lead.email) = recent_in_product_trial_requests.email
    where contact.id is null
)
select * from map_to_leads