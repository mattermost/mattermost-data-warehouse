{{config({
    "schema": "hightouch",
    "materialized": "view",
    "tags":["hourly","blapi"]
  })
}}

WITH customers_with_cloud_paid_subs as (
    SELECT
        customers_with_cloud_paid_subs.*,
        'Customer' as account_type,
        -- Metadata to be used for selecting proper external id
        external_id_account.id as dwh_id_account_id,
        external_id_account.id is not null as has_dwh_id_account_id,
        external_id_account.dwh_external_id__c as dwh_id_account_external_id,

        contact.accountid as contact_account_id,
        contact.accountid is not null as has_contact_match,
        contact_account.dwh_external_id__c as contact_account_external_id,

        lead.existing_account__c is not null as has_lead_match,
        lead.existing_account__c as lead_account_id,
        lead_account.dwh_external_id__c as lead_account_external_id,

        domain_account.id is not null as has_domain_match,
        domain_account.id as domain_account_id,
        domain_account.dwh_external_id__c as domain_account_external_id

    FROM {{ ref('customers_with_cloud_paid_subs') }}
       -- Case 1: matching account from external_id
        LEFT JOIN {{ ref('account') }} external_id_account ON customers_with_cloud_paid_subs.account_external_id = external_id_account.dwh_external_id__c
        -- Case 2: matching account from contact
        LEFT JOIN {{ ref('contact') }} ON customers_with_cloud_paid_subs.email = contact.email and contact.accountid is not null
        LEFT JOIN {{ ref('account') }} contact_account ON contact.accountid = contact_account.id
        -- Case 3: matching account from lead
        LEFT JOIN {{ ref('lead') }} on customers_with_cloud_paid_subs.email = lead.email and lead.existing_account__c is not null
        LEFT JOIN {{ ref('account') }} lead_account ON lead.existing_account__c = lead_account.id
        -- Case 4: matching account from domain
        LEFT JOIN {{ ref('account') }} domain_account ON customers_with_cloud_paid_subs.domain = domain_account.cbit__clearbitdomain__c


    WHERE
    customers_with_cloud_paid_subs.hightouch_sync_eligible
    AND customers_with_cloud_paid_subs.status in ('canceled', 'active') 
    AND customers_with_cloud_paid_subs.SKU = 'Cloud Professional'
), customer_to_account as (
    SELECT
        -- Data for debugging
        email,
        dwh_id_account_id,
        dwh_id_account_external_id,
        contact_account_id,
        contact_account_external_id,
        lead_account_id,
        lead_account_external_id,
        domain_account_id,
        domain_account_external_id,
        -- Data to be updated
        -- Select account id based on priority:
        --  1. external id match
        --  2. account from contact
        --  3. account from lead
        --  4. account with matching domain
        case
            when has_external_id_match then 'existing external id'
            when has_contact_match then 'contact'
            when has_lead_match then 'lead'
            else 'domain'
        end as external_id_from,
        -- Define priority as a number. The lower the number the higher the priority
        case
            when has_external_id_match then 1
            when has_contact_match then 2
            when has_lead_match then 3
            else 4
        end as priority,
        case
            when has_external_id_match then dwh_id_account_external_id
            when has_contact_match then contact_account_external_id
            when has_lead_match then lead_account_external_id
            else domain_account_external_id
        end as account_external_id,
        account_type
    FROM customers_with_cloud_paid_subs
)
select
    *
from
    customer_to_account
-- Remove duplicates by prioritizing over external id, contact, lead and finally domain.
qualify row_number() over(
    partition by account_external_id
    order by priority
) = 1