{{config({
    "materialized": 'table',
    "schema": "hightouch"
  })
}}

with existing_members as (
    select
        campaignmember.sfid,
        campaignmember.email,
        campaignmember.dwh_external_id__c,
        case
            when campaignmember.campaignid = '7013p000001NkNtAAK' then 'Portal'
            when campaignmember.campaignid = '7013p000001Ttg5AAC' then 'Cloud'
        end as campaign_type,
        row_number() over (partition by campaignmember.email order by createddate desc) as row_num
    from {{ ref('campaignmember') }}
    where campaignmember.campaignid = '7013p000001TuhdAAC'
), existing_leads as (
    select
        lead.sfid,
        lead.email,
        lead.dwh_external_id__c,
        row_number() over (partition by lead.email order by createddate desc) as row_num
    from {{ ref('lead') }}
), existing_contacts as (
    select
        contact.sfid,
        contact.email,
        contact.dwh_external_id__c,
        row_number() over (partition by contact.email order by createddate desc) as row_num
    from {{ ref('contact') }}
)
select
    facts.email,
    'Sales Inquiry' as contact_us_inquiry_type,
    to_varchar(facts.created_at, 'YYYY-MM-DDTHH24:MI:SSZ') as request_to_contact_us_date,
    facts.comment as tell_us_more,
    facts.name as company,
    lead.dwh_external_id__c is not null as lead_exists,
    contact.dwh_external_id__c is not null as contact_exists,
    coalesce(campaignmember.dwh_external_id__c, UUID_STRING('78157189-82de-4f4d-9db3-88c601fbc22e', '7013p000001TxBuAAK' || facts.email)) AS campaignmember_external_id,
    coalesce(contact.dwh_external_id__c, UUID_STRING('78157189-82de-4f4d-9db3-88c601fbc22e', facts.email)) AS contact_external_id,
    coalesce(lead.dwh_external_id__c, UUID_STRING('78157189-82de-4f4d-9db3-88c601fbc22e', facts.email)) AS lead_external_id,
    '0051R00000GnvhhQAB' as lead_ownerid,
    'Contact Request' as most_recent_action,
    case
        when facts.source = 'Portal' then 'In-Portal Contact Us'
        when facts.source = 'Cloud' then 'In-Cloud Contact Us'
    end as action_detail,
    case
        when facts.source = 'Portal' then 'MM Customer Portal'
        when facts.source = 'Cloud' then 'MM Cloud'
    end as lead_source_detail,
    'Referral' as lead_source,
    'Responded' as campaignmember_status,
    false as marketing_suspend,
    case
        when facts.source = 'Portal' then '7013p000001NkNtAAK'
        when facts.source = 'Cloud' then '7013p000001Ttg5AAC'
    end as campaign_id,
    campaignmember.sfid as campaignmember_sfid,
    lead.sfid as lead_sfid,
    contact.sfid as contact_sfid
from
    {{ ref('contact_us_requests') }} facts
    left join existing_members as campaignmember
        on facts.email = campaignmember.email and campaignmember.row_num = 1
    left join existing_leads as lead
        on facts.email = lead.email and lead.row_num = 1
    left join existing_contacts as contact
        on facts.email = contact.email and contact.row_num = 1
where
    facts.inquiry_type = 'I need to contact sales'
    and facts.inquiry_issue != 'I want to cancel my Mattermost account'
    and facts.created_at >= '2021-08-23'