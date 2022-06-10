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
    facts.dns,
    facts.stripe_customer_id,
    facts.installation_id,
    facts.trial_start,
    facts.trial_end,
    facts.last_active_date,
    facts.cloud_posts_total,
    facts.cloud_cards_total,
    facts.cloud_plugins_total,
    facts.cloud_installation_state,
    facts.cloud_posts_daily,
    facts.cloud_mau,
    facts.cloud_dau,
    left(coalesce(facts.company_name, facts.email), 40) as company_name,
    lead.dwh_external_id__c is not null as lead_exists,
    contact.dwh_external_id__c is not null as contact_exists,
    coalesce(campaignmember.dwh_external_id__c, UUID_STRING('78157189-82de-4f4d-9db3-88c601fbc22e', '7013p000001TxBuAAK' || facts.portal_customer_id || facts.email)) AS campaignmember_external_id,
    coalesce(contact.dwh_external_id__c, UUID_STRING('78157189-82de-4f4d-9db3-88c601fbc22e', facts.portal_customer_id || facts.email)) AS contact_external_id,
    coalesce(lead.dwh_external_id__c, UUID_STRING('78157189-82de-4f4d-9db3-88c601fbc22e', facts.portal_customer_id || facts.email)) AS lead_external_id,
    '0051R00000GnvhhQAB' as lead_ownerid,
    'Cloud PQL' as most_recent_action,
    'Cloud Workspace Creation' as action_detail,
    'Referral' as lead_source,
    'MM Cloud' as lead_source_detail,
    false as marketing_suspend,
    '7013p000001TuhdAAC' as campaign_id,
    '70118000000RMdfAAG' as sandbox_campaign_id,
    campaignmember.sfid as campaignmember_sfid,
    lead.sfid as lead_sfid,
    contact.sfid as contact_sfid,
    case
        when facts.dns is not null then 'Completed Signup'
        when facts.completed_signup_at is not null then 'Completed Signup'
        when facts.workspace_provisioning_started_at is not null then 'Created Workspace'
        when facts.entered_company_name_at is not null then 'Entered Company Name'
        when facts.verified_email_at is not null then 'Verified Email'
        when facts.submitted_form_at is not null then 'Submitted Form'
    end as campaign_status,
    coalesce(
        facts.completed_signup_at,
        facts.workspace_provisioning_started_at,
        facts.entered_company_name_at,
        facts.verified_email_at,
        facts.submitted_form_at) as last_action_date
from
    {{ ref('cloud_signup_campaign_facts') }} facts
    left join existing_members as campaignmember
        on facts.email = campaignmember.email and campaignmember.row_num = 1
    left join existing_leads as lead
        on facts.email = lead.email and lead.row_num = 1
    left join existing_contacts as contact
        on facts.email = contact.email and contact.row_num = 1