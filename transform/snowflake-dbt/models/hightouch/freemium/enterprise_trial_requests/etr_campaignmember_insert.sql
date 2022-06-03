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
    where campaignmember.campaignid = '7016u0000002OHjAAM'
    -- where campaignmember.campaignid = '7013p000001U2GbAAK'
), existing_leads as (
    select
        lead.sfid,
        lead.email,
        lead.dwh_external_id__c,
        row_number() over (partition by lead.email order by createddate desc) as row_num
    from {{ ref('lead') }}
    where lead.CAMPAIGN_ID__C = '7016u0000002OHjAAM'
    -- where lead.CAMPAIGN_ID__C = '7013p000001U2GbAAK'  
), existing_contacts as (
    select
        contact.sfid,
        contact.email,
        contact.dwh_external_id__c,
        row_number() over (partition by contact.email order by createddate desc) as row_num
    from {{ ref('contact') }}
    where contact.CAMPAIGN_ID__C = '7016u0000002OHjAAM'
    -- where contact.CAMPAIGN_ID__C = '7013p000001U2GbAAK'
WITH campaignmembers_to_sync as (
    SELECT
        customers_with_freemium_subs.email,
        COALESCE(existing_contacts.dwh_external_id__c, customers_with_freemium_subs.contact_external_id) as contact_external_id,
        COALESCE(
            existing_leads.dwh_external_id__c,
            UUID_STRING(
                '78157189-82de-4f4d-9db3-88c601fbc22e',
                customers_with_freemium_subs.customer_id || customers_with_freemium_subs.email)
        ) AS lead_external_id,
        '7016u0000002OHjAAM' as sandbox_campaign_id,
        '7013p000001U2GbAAK' as campaign_id,
        'TBD' as campaignmember_status
    FROM {{ ref('customers_with_freemium_subs') }}
    LEFT JOIN existing_members ON customers_with_freemium_subs.email = existing_members.email
    LEFT JOIN existing_leads ON customers_with_freemium_subs.email = existing_leads.email
    LEFT JOIN existing_contacts ON customers_with_freemium_subs.email = existing_contacts.email
    existing_leads.id is null and existing_contacts.id is null
    AND customers_with_freemium_subs.hightouch_sync_eligible

SELECT * FROM campaignmembers_to_sync