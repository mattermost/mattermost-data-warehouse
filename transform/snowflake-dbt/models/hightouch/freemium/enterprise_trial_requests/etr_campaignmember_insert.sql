{{config({
    "schema": "hightouch",
    "materialized": "view",
    "tags":["hourly","freemium"]
  })
}}
with existing_members as (
    select
        campaignmember.sfid,
        campaignmember.email,
        campaignmember.dwh_external_id__c,
        row_number() over (partition by campaignmember.email order by createddate desc) as row_num
    from {{ ref('campaignmember') }}
    -- where campaignmember.campaignid = '7016u0000002OHjAAM' (sandbox id)
    where campaignmember.campaignid = '7013p000001U2GbAAK'
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
), campaignmembers_to_sync as (
    SELECT
        existing_leads.sfid as lead_sfid,
        existing_contacts.sfid as contact_sfid,
        customers_with_free_subs.email,
        customers_with_free_subs.company_name,
        COALESCE(
            existing_contacts.dwh_external_id__c,
            UUID_STRING(
                '78157189-82de-4f4d-9db3-88c601fbc22e',
                customers_with_free_subs.customer_id || customers_with_free_subs.email)
        ) AS contact_external_id,
        COALESCE(
            existing_leads.dwh_external_id__c,
            UUID_STRING(
                '78157189-82de-4f4d-9db3-88c601fbc22e',
                customers_with_free_subs.customer_id || customers_with_free_subs.email)
        ) AS lead_external_id,
        '7016u0000002OHjAAM' as sandbox_campaign_id,
        '7013p000001U2GbAAK' as campaign_id,
        'Responded' as campaignmember_status
    FROM {{ ref('customers_with_cloud_free_subs') }} as customers_with_free_subs
    LEFT JOIN existing_members ON customers_with_free_subs.email = existing_members.email
    LEFT JOIN existing_leads ON customers_with_free_subs.email = existing_leads.email
    LEFT JOIN existing_contacts ON customers_with_free_subs.email = existing_contacts.email
    WHERE (existing_leads.sfid is not null or existing_contacts.sfid is not null) -- either contact or lead exists
    AND existing_members.sfid is null -- not member of campaign either
    AND sku = 'Cloud Enterprise' 
    AND previous_sku = 'Cloud Starter'
    AND customers_with_free_subs.status = 'trialing'
    AND customers_with_free_subs.hightouch_sync_eligible
)
SELECT * FROM campaignmembers_to_sync