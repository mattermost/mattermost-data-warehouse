{{config({
    "materialized": 'table',
    "schema": "hightouch"
  })
}}

select
    facts.email,
    facts.dns,
    facts.stripe_customer_id,
    left(facts.company_name, 40) as company_name,
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
    left join {{ ref('campaignmember') }}
        on facts.email = campaignmember.email and campaignmember.campaignid = '7013p000001TuhdAAC'
    left join {{ ref('lead') }} on facts.email = lead.email
    left join {{ ref('contact') }} on facts.email = contact.email