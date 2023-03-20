{{ config(
    { "schema": "hightouch",
    "materialized": "view",
    "tags" :["hourly","blapi"] }
) }}
-- lead or contact already exists
WITH existing_leads AS (

    SELECT
        LEAD.id,
        LEAD.email,
        LEAD.ownerid
    FROM
        {{ ref('lead') }}
    WHERE
        converteddate IS NULL qualify ROW_NUMBER() over (
            PARTITION BY LEAD.email
            ORDER BY
                LEAD.createddate DESC
        ) = 1
),
freemium_contacts_to_sync AS (
    SELECT
        customers_with_cloud_paid_subs.email,
        customers_with_cloud_paid_subs.first_name,
        COALESCE(
            customers_with_cloud_paid_subs.last_name,
            customers_with_cloud_paid_subs.email
        ) AS contact_last_name,
        existing_lead.id AS duplicate_lead_id,
        -- existing_lead.email AS duplicate_lead_email,
        customers_with_cloud_paid_subs.account_external_id,
        {{ get_ownerid_or_default('existing_leads.ownerid') }} AS ownerid,
        'Cloud Purchase' AS most_recent_action,
        'Cloud Professional' AS most_recent_action_detail,
        'Referral' AS lead_source,
        'Cloud Starter' AS lead_source_detail
    FROM
        {{ ref('customers_with_cloud_paid_subs') }}
        LEFT JOIN {{ ref('contact') }}
        ON customers_with_cloud_paid_subs.email = contact.email
        LEFT JOIN existing_lead
        ON customers_with_cloud_paid_subs.email = existing_lead.email
    WHERE
        contact.id IS NOT NULL
        AND customers_with_cloud_paid_subs.hightouch_sync_eligible
        AND customers_with_cloud_paid_subs.status IN (
            'canceled',
            'active'
        )
        AND customers_with_cloud_paid_subs.sku = 'Cloud Professional'
)
SELECT
    *
FROM
    freemium_contacts_to_sync
