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
        row_number() over (partition by campaignmember.email order by createddate desc) as row_num
    from {{ ref('campaignmember') }}
    where campaignmember.campaignid = '7013p000001U28AAAS'
), existing_leads as (
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
)
select
    facts.email,
    facts.dns,
    facts.stripe_customer_id,
    facts.installation_id,
    facts.trial_start,
    facts.trial_end,
    facts.last_active_date,
    facts.cloud_posts_total,
    facts.cloud_posts_daily,
    facts.cloud_mau,
    facts.cloud_dau,
    left(coalesce(facts.company_name, facts.email), 40) as company_name,
    lead.dwh_external_id__c is not null or l2.dwh_external_id__c is not null as lead_exists,
    contact.dwh_external_id__c is not null as contact_exists,
    coalesce(campaignmember.dwh_external_id__c, UUID_STRING('78157189-82de-4f4d-9db3-88c601fbc22e', '7013p000001TxBuAAK' || facts.portal_customer_id || facts.email)) AS campaignmember_external_id,
    coalesce(contact.dwh_external_id__c, UUID_STRING('78157189-82de-4f4d-9db3-88c601fbc22e', facts.portal_customer_id || facts.email)) AS contact_external_id,
    coalesce(lead.dwh_external_id__c, UUID_STRING('78157189-82de-4f4d-9db3-88c601fbc22e', facts.portal_customer_id || facts.email)) AS lead_external_id,
    '0051R00000GnvhhQAB' as lead_ownerid,
    'Cloud Starter Signup' as most_recent_action,
    CASE when sso_provider is not null then 'SSO' else 'Email' END as most_recent_action_detail,
    'Cloud Workspace Creation' as action_detail,
    coalesce(lead.LEAD_SOURCE_TEXT__C,'Referral') as lead_source,
    coalesce(lead.LEAD_SOURCE_DETAIL__C,'Mattermost Cloud') as lead_source_detail,
    false as marketing_suspend,
    '7013p000001U28AAAS' as campaign_id,
    '7016u0000002Q5zAAE' as sandbox_campaign_id,
    campaignmember.sfid as campaignmember_sfid,
    case when facts.sso_provider is not null then true else false end as is_sso,
    coalesce(facts.sso_provider, 'Email') as signup_method,
    lead.sfid as lead_sfid,
    contact.sfid as contact_sfid,
    case
        when facts.dns is not null then 'Workspace Created'
        when facts.workspace_created_at is not null then 'Workspace Created'
        when facts.verified_email_at is not null then 'Email Verified'
        when facts.account_created_at is not null then 'Account Created'
    end as campaign_status,
    coalesce(
        facts.account_created_at,
        facts.verified_email_at,
        facts.workspace_created_at,
        facts.workspace_provisioning_started_at
        ) as last_action_date
from
    {{ ref('cs_signup_campaign_facts') }} facts
    left join existing_members as campaignmember
        on facts.email = campaignmember.email and campaignmember.row_num = 1
    left join existing_leads as lead
        on facts.email = lead.email and lead.row_num = 1
    left join existing_leads as l2
        on UUID_STRING('78157189-82de-4f4d-9db3-88c601fbc22e', facts.portal_customer_id || facts.email) = l2.dwh_external_id__c and l2.row_num = 1
    left join existing_contacts as contact
        on facts.email = contact.email and contact.row_num = 1