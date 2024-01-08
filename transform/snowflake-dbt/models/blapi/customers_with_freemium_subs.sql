{{
  config({
    "schema": "hightouch",
    "unique_key":"id",
    "tags": ["hourly", "blapi", "deprecated"]
  })
}}

WITH latest_credit_card_address AS (
    SELECT
        payment_methods.customer_id,
        addresses.line1,
        addresses.line2,
        addresses.postal_code,
        addresses.city,
        addresses.state,
        coalesce(postal_code_mapping.country_name, addresses.country) as country,
        coalesce(postal_code_mapping.state_code, addresses.state) as state_code,
        coalesce(postal_code_mapping.country, addresses.country) as country_code,
        ROW_NUMBER() OVER (PARTITION BY payment_methods.customer_id ORDER BY payment_methods.created_at DESC) as row_num
    FROM {{ ref('credit_cards') }}
    JOIN {{ ref('payment_methods') }} ON credit_cards.id = payment_methods.id
    JOIN {{ ref('addresses') }} ON payment_methods.address_id = addresses.id
    LEFT JOIN {{ source('util', 'postal_code_mapping') }}
        ON addresses.country = postal_code_mapping.country
        AND (
                postal_code_mapping.postal_code like addresses.postal_code || '%'
                OR
                -- UK zip codes are weird and we can best match on the first 4 alphanumerics
                (addresses.country = 'GB' AND postal_code_mapping.postal_code like left(addresses.postal_code, 4) || '%')
        )
    WHERE addresses.address_type = 'billing'
    -- when they subscribe to paid version of cloud (freemium)
), freemium_subscriptions AS (
    SELECT DISTINCT
        freemium_subscriptions.*
    FROM
        {{ ref('freemium_subscriptions') }}
), customers_with_freemium_subs AS (
    SELECT
        customers.id as customer_id,
        customers.email,
        customers.first_name,
        coalesce(NULLIF(TRIM(customers.last_name), ''), customers.email) as last_name,
        CASE WHEN SPLIT_PART(customers.email, '@', 2) = 'gmail.com' 
        THEN NULL        
        ELSE SPLIT_PART(customers.email, '@', 2) END as domain,
        freemium_subscriptions.id as subscription_id,
        freemium_subscriptions.subscription_version_id,
        freemium_subscriptions.previous_subscription_version_id,
        freemium_subscriptions.cloud_dns,
        to_varchar(freemium_subscriptions.start_date, 'yyyy-mm-dd"T"hh24:mi:ss"Z"') as start_date,
        to_varchar(DATEADD(year, -1, freemium_subscriptions.start_date), 'yyyy-mm-dd"T"hh24:mi:ss"Z"') as end_date,
        to_varchar(freemium_subscriptions.updated_at, 'yyyy-mm-dd"T"hh24:mi:ss"Z"') as updated_at,
        true as true_boolean,
        freemium_subscriptions.invoice_number,
        freemium_subscriptions.stripe_charge_id,
        freemium_subscriptions.num_seats,
        freemium_subscriptions.sku,
        latest_credit_card_address.line1,
        latest_credit_card_address.line2,
        latest_credit_card_address.line1 || ' ' || coalesce(latest_credit_card_address.line2, '') as street_address,
        latest_credit_card_address.postal_code,
        latest_credit_card_address.city,
        case
            when latest_credit_card_address.country in ('US', 'CA')
            then latest_credit_card_address.state
            else null
        end as state,
        latest_credit_card_address.country,
        case
            when latest_credit_card_address.country in ('US', 'CA')
            then latest_credit_card_address.state_code
            else null
        end as state_code,
        case
            when latest_credit_card_address.country in ('US', 'CA')
            then latest_credit_card_address.country_code
            else null
        end as country_code,
        freemium_subscriptions.status,
        freemium_subscriptions.updated_at >= '2021-08-18' as hightouch_sync_eligible
    FROM {{ ref('customers_blapi') }} customers
        JOIN freemium_subscriptions AS freemium_subscriptions ON customers.id = freemium_subscriptions.customer_id
        JOIN {{ source('blapi', 'products') }} ON freemium_subscriptions.sku = products.sku
        LEFT JOIN {{ source('salesforce','lead')}} on lead.email = customers.email
        LEFT JOIN latest_credit_card_address
            ON customers.id = latest_credit_card_address.customer_id
            AND latest_credit_card_address.row_num = 1
        WHERE lead.email is not null -- filtering out accounts that do not have leads.
), customers_account AS (
    SELECT
        customers_with_freemium_subs.subscription_id,
        COALESCE(
            COALESCE(account.dwh_external_id__c, account_domain_mapping.account_external_id),
            UUID_STRING(
                '78157189-82de-4f4d-9db3-88c601fbc22e',
                customers_with_freemium_subs.customer_id)
        ) AS account_external_id,
        COALESCE(account.sfid, account_domain_mapping.accountid) as account_sfid,
        ROW_NUMBER() OVER (PARTITION BY customers_with_freemium_subs.subscription_id ORDER BY account.lastmodifieddate DESC) as row_num
    FROM customers_with_freemium_subs
    LEFT JOIN {{ ref('account') }}
        ON customers_with_freemium_subs.domain = account.cbit__clearbitdomain__c
    LEFT JOIN {{ source('orgm', 'account_domain_mapping') }}
        ON customers_with_freemium_subs.domain = account_domain_mapping.domain
), customers_contact AS (
    SELECT
        customers_with_freemium_subs.subscription_id,
        COALESCE(
            contact.dwh_external_id__c,
            UUID_STRING(
                '78157189-82de-4f4d-9db3-88c601fbc22e',
                customers_with_freemium_subs.customer_id || customers_with_freemium_subs.email)
        ) AS contact_external_id,
        contact.sfid as contact_sfid,
        account.id as account_sfid,
        account.dwh_external_id__c as contact_account_external_id,
        ROW_NUMBER() OVER (PARTITION BY customers_with_freemium_subs.subscription_id ORDER BY contact.lastmodifieddate DESC) as row_num
    FROM customers_with_freemium_subs
    LEFT JOIN {{ ref('contact') }}
       ON customers_with_freemium_subs.email = contact.email
    LEFT JOIN {{ ref('account') }}
        ON contact.accountid = account.id
), customers_opportunity AS (
    SELECT
        customers_with_freemium_subs.subscription_id,
        COALESCE(
            opportunity.dwh_external_id__c,
            UUID_STRING(
                '78157189-82de-4f4d-9db3-88c601fbc22e',
                customers_with_freemium_subs.subscription_id)
        ) AS opportunity_external_id,
        opportunity.sfid as opportunity_sfid,
        ROW_NUMBER() OVER (PARTITION BY customers_with_freemium_subs.subscription_id ORDER BY opportunity.lastmodifieddate DESC) as row_num
    FROM customers_with_freemium_subs
    LEFT JOIN {{ ref('opportunity') }}
        ON customers_with_freemium_subs.subscription_id = opportunity.subs_id__c
            AND customers_with_freemium_subs.end_date = opportunity.closedate
)
SELECT
    customers_with_freemium_subs.*,
    case
        when customers_account.account_sfid is null and customers_contact.account_sfid is not null
        then customers_contact.contact_account_external_id
        else customers_account.account_external_id
    end as account_external_id,
    coalesce(customers_account.account_sfid, customers_contact.account_sfid) as account_sfid,
    customers_contact.contact_external_id,
    customers_contact.contact_sfid,
    customers_opportunity.opportunity_external_id,
    customers_opportunity.opportunity_sfid
FROM customers_with_freemium_subs
JOIN customers_account
    ON customers_with_freemium_subs.subscription_id = customers_account.subscription_id
    AND customers_account.row_num = 1
JOIN customers_contact
    ON customers_with_freemium_subs.subscription_id = customers_contact.subscription_id
    AND customers_contact.row_num = 1
JOIN customers_opportunity
    ON customers_with_freemium_subs.subscription_id = customers_opportunity.subscription_id
    AND customers_opportunity.row_num = 1