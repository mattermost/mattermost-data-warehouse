{{config({
    "schema": "hightouch",
    "materialized": "view",
    "tags":["hourly", "blapi", "deprecated"]
  })
}}

WITH cloud_ocrs_to_sync as (
    SELECT
        customers_with_cloud_subs.*,
        '0053p0000064nt8AAA' AS ownerid
    FROM {{ ref('customers_with_cloud_subs') }}
    LEFT JOIN {{ ref('opportunity') }}
        ON customers_with_cloud_subs.opportunity_external_id = opportunity.dwh_external_id__c
            OR customers_with_cloud_subs.subscription_id = opportunity.subs_id__c
    LEFT JOIN {{ ref('contact') }}
        ON customers_with_cloud_subs.email = contact.email
            OR customers_with_cloud_subs.contact_external_id = contact.dwh_external_id__c
    LEFT JOIN {{ ref('opportunitycontactrole') }}
        ON contact.sfid = opportunitycontactrole.contactid
            AND opportunity.sfid = opportunitycontactrole.opportunityid
    WHERE opportunity.sfid IS NULL AND contact.sfid IS NULL AND opportunitycontactrole.sfid IS NULL
    AND customers_with_cloud_subs.hightouch_sync_eligible
)
SELECT *,
    UUID_STRING(
        '78157189-82de-4f4d-9db3-88c601fbc22e',
        cloud_ocrs_to_sync.contact_external_id || 'billingcontact')
    AS ocr_external_id,
    'Billing Contact' as role,
    true as isprimary
FROM cloud_ocrs_to_sync