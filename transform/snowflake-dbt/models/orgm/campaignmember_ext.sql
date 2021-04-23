
{{config({
    "materialized": "table",
    "schema": "orgm"
  })
}}

WITH campaignmember_ext AS (
    SELECT
        campaignmember.sfid as campaignmember_sfid,
        COALESCE(contact.request_to_contact_us_date__c,lead.request_to_contact_us_date__c) AS contact_us_request_date,
        COALESCE(contact.trial_req_date__c,lead.request_a_trial_date__c) AS trial_request_date
    FROM {{ ref('campaignmember') }}
    LEFT JOIN {{ ref('lead') }} ON campaignmember.leadid = lead.sfid
    LEFT JOIN {{ ref('contact') }} ON campaignmember.contactid = contact.sfid
    WHERE COALESCE(contact.request_to_contact_us_date__c,lead.request_to_contact_us_date__c) IS NOT NULL 
        OR COALESCE(contact.trial_req_date__c,lead.request_a_trial_date__c) IS NOT NULL
)

SELECT *
FROM campaignmember_ext