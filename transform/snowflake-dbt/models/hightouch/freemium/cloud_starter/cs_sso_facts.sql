{{config({
    "materialized": 'table',
    "schema": "hightouch"
  })
}}
-- temp table, will be deleted once null customer_portal_id for leads is fixed.

with existing_leads as (
    select
        lead.sfid,
        lead.email,
        lead.dwh_external_id__c,
        lead.LEAD_SOURCE_TEXT__C,
        lead.LEAD_SOURCE_DETAIL__C,
        row_number() over (partition by lead.email order by createddate desc) as row_num
    from {{ ref('lead') }}
), existing_contacts as (
    select
        contact.sfid,
        contact.email,
        contact.dwh_external_id__c,
        row_number() over (partition by contact.email order by createddate desc) as row_num
    from {{ ref('contact') }}
), existing_members as (
    select
        campaignmember.sfid,
        campaignmember.email,
        campaignmember.dwh_external_id__c,
        row_number() over (partition by campaignmember.email order by createddate desc) as row_num
    from {{ ref('campaignmember') }}
    where campaignmember.campaignid = '7013p000001U28AAAS'
), sso_facts_pre as (
    select 
    customers.email,
    customers.name as company_name,
    customers.id as stripe_customer_id,
    customers.updated >= '2022-06-14' as lead_sync_eligible,
    subscriptions.cws_installation as installation_id,
    subscriptions.cws_dns as dns,
    subscriptions.created,
    subscriptions.trial_start,
    subscriptions.trial_end,
    coalesce(campaignmember.dwh_external_id__c, UUID_STRING('78157189-82de-4f4d-9db3-88c601fbc22e', '7013p000001TxBuAAK' || customers.email)) AS campaignmember_external_id,
    coalesce(contact.dwh_external_id__c, UUID_STRING('78157189-82de-4f4d-9db3-88c601fbc22e', customers.email)) AS contact_external_id,
    coalesce(lead.dwh_external_id__c, UUID_STRING('78157189-82de-4f4d-9db3-88c601fbc22e', customers.email)) AS lead_external_id,
    lead.dwh_external_id__c is not null as lead_exists,
    contact.dwh_external_id__c is not null as contact_exists,
    campaignmember.sfid as campaignmember_sfid,
    '7013p000001U28AAAS' as campaign_id,
    '7016u0000002Q5zAAE' as sandbox_campaign_id,
    '0051R00000GnvhhQAB' as lead_ownerid,
    'Cloud Starter Signup' as most_recent_action,
    false as marketing_suspend,
    lead.sfid as lead_sfid,
    case
        when subscriptions.cws_dns is not null then 'Completed Signup'
        else 'Account Created' 
    end as campaign_status
    from 
        {{ ref('customers') }}
        left join {{ ref('subscriptions') }} on customers.id = subscriptions.customer
        left join existing_leads as lead on customers.email = lead.email and lead.row_num = 1
        left join existing_contacts as contact on customers.email = contact.email and contact.row_num = 1
        left join existing_members as campaignmember on customers.email = campaignmember.email and campaignmember.row_num = 1
        where customers.email not in ( select email from {{ ref('cs_signup_campaign')}} )
        and lower(subscriptions.edition) = 'cloud starter' and lead_sync_eligible 
)
select * from sso_facts_pre