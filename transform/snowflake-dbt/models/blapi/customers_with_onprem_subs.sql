{{config({
    "schema": "hightouch",
    "unique_key":"id",
    "tags":["hourly","blapi"]
  })
}}

WITH latest_credit_card_address AS (
    SELECT
        payment_methods.customer_id,
        addresses.line1,
        addresses.line2,
        addresses.postal_code,
        addresses.city,
        coalesce(postal_code_mapping.state_code, addresses.state) as state,
        coalesce(postal_code_mapping.country, addresses.country) as country,
        ROW_NUMBER() OVER (PARTITION BY payment_methods.customer_id ORDER BY payment_methods.created_at DESC) as row_num
    FROM {{ ref('credit_cards') }}
    JOIN {{ ref('payment_methods') }} ON credit_cards.id = payment_methods.id
    JOIN {{ ref('addresses') }} ON payment_methods.address_id = addresses.id
    LEFT JOIN {{ source('util', 'postal_code_mapping') }}
        ON addresses.country = postal_code_mapping.country
        AND postal_code_mapping.postal_code like addresses.postal_code || '%'
    WHERE addresses.address_type = 'billing'
), customers_with_onprem_subs AS (
    SELECT
        customers.id as customer_id,
        customers.email,
        customers.first_name,
        coalesce(customers.last_name, customers.email) as last_name,
        SPLIT_PART(customers.email, '@', 2) as domain,
        onprem_subscriptions.id as subscription_id,
        onprem_subscriptions.subscription_version_id,
        onprem_subscriptions.previous_subscription_version_id,
        onprem_subscriptions.start_date,
        onprem_subscriptions.end_date,
        onprem_subscriptions.total_in_cents / 100.0 as total,
        onprem_subscriptions.updated_at,
        onprem_subscriptions.invoice_number,
        onprem_subscriptions.stripe_charge_id,
        onprem_subscriptions.num_seats,
        onprem_subscriptions.sku,
        products.pricebookentryid,
        subscriptions.actual_renewal_date,
        dateadd(day, 1, subscriptions.actual_renewal_date::date) as renewal_start_date,
        dateadd(year, 1, subscriptions.actual_renewal_date::date) as renewal_end_date,
        renewed_from_subscription.sfdc_migrated_opportunity_sfid,
        coalesce(renewed_from_subscription.total_in_cents, 0) / 100.0 as renewed_from_total,
        COALESCE(subscriptions.license_id, onprem_subscriptions.id) as license_key,
        subscriptions.purchase_order_num,
        latest_credit_card_address.line1,
        latest_credit_card_address.line2,
        latest_credit_card_address.postal_code,
        latest_credit_card_address.city,
        latest_credit_card_address.state,
        latest_credit_card_address.country,
        onprem_subscriptions.updated_at >= '2021-08-23' as hightouch_sync_eligible
    FROM {{ ref('customers_blapi') }} customers
        JOIN {{ ref('onprem_subscriptions') }} ON customers.id = onprem_subscriptions.customer_id
        JOIN {{ ref('subscriptions') }} ON onprem_subscriptions.stripe_id = subscriptions.id
        JOIN {{ source('blapi', 'products') }} ON onprem_subscriptions.sku = products.sku
        LEFT JOIN {{ ref('subscriptions') }} renewed_from_subscription
            ON subscriptions.renewed_from_sub_id = renewed_from_subscription.id
        JOIN latest_credit_card_address
            ON customers.id = latest_credit_card_address.customer_id
            AND latest_credit_card_address.row_num = 1
), customers_account AS (
    SELECT
        customers_with_onprem_subs.stripe_charge_id,
        COALESCE(
            account.dwh_external_id__c,
            UUID_STRING(
                '78157189-82de-4f4d-9db3-88c601fbc22e',
                customers_with_onprem_subs.customer_id)
        ) AS account_external_id,
        account.sfid as account_sfid,
        ROW_NUMBER() OVER (PARTITION BY account.cbit__clearbitdomain__c ORDER BY account.lastmodifieddate DESC) as row_num
    FROM customers_with_onprem_subs
    LEFT JOIN {{ ref('account') }}
        ON customers_with_onprem_subs.domain = account.cbit__clearbitdomain__c
), customers_contact AS (
    SELECT
        customers_with_onprem_subs.stripe_charge_id,
        COALESCE(
            contact.dwh_external_id__c,
            UUID_STRING(
                '78157189-82de-4f4d-9db3-88c601fbc22e',
                customers_with_onprem_subs.customer_id || customers_with_onprem_subs.email)
        ) AS contact_external_id,
        contact.sfid as contact_sfid,
        ROW_NUMBER() OVER (PARTITION BY contact.email ORDER BY contact.lastmodifieddate DESC) as row_num
    FROM customers_with_onprem_subs
    LEFT JOIN {{ ref('contact') }}
       ON customers_with_onprem_subs.email = contact.email
), customers_opportunity AS (
    SELECT
        customers_with_onprem_subs.stripe_charge_id,
        COALESCE(
            opportunity.dwh_external_id__c,
            UUID_STRING(
                '78157189-82de-4f4d-9db3-88c601fbc22e',
                customers_with_onprem_subs.subscription_version_id)
        ) AS opportunity_external_id,
        opportunity.sfid as opportunity_sfid,
        ROW_NUMBER() OVER (PARTITION BY opportunity.stripe_id__c ORDER BY opportunity.lastmodifieddate DESC) as row_num
    FROM customers_with_onprem_subs
    LEFT JOIN {{ ref('opportunity') }}
        ON customers_with_onprem_subs.stripe_charge_id = opportunity.stripe_id__c
), customers_oli AS (
    SELECT
        customers_with_onprem_subs.stripe_charge_id,
        COALESCE(
            opportunitylineitem.dwh_external_id__c,
            UUID_STRING('78157189-82de-4f4d-9db3-88c601fbc22e', customers_opportunity.opportunity_external_id || 'oli')
        ) AS opportunitylineitem_external_id,
        opportunitylineitem.sfid as opportunitylineitem_sfid,
        ROW_NUMBER() OVER (PARTITION BY opportunitylineitem.subs_version_id__c ORDER BY opportunitylineitem.lastmodifieddate DESC) as row_num
    FROM customers_with_onprem_subs
    LEFT JOIN customers_opportunity
        ON customers_with_onprem_subs.stripe_charge_id = customers_opportunity.stripe_charge_id
    LEFT JOIN {{ ref('opportunitylineitem') }}
        ON customers_with_onprem_subs.subscription_version_id = opportunitylineitem.subs_version_id__c
)
SELECT
    customers_with_onprem_subs.*,
    customers_account.account_external_id,
    customers_account.account_sfid,
    customers_contact.contact_external_id,
    customers_contact.contact_sfid,
    customers_opportunity.opportunity_external_id,
    customers_opportunity.opportunity_sfid,
    customers_oli.opportunitylineitem_external_id,
    customers_oli.opportunitylineitem_sfid
FROM customers_with_onprem_subs
JOIN customers_account
    ON customers_with_onprem_subs.stripe_charge_id = customers_account.stripe_charge_id
    AND customers_account.row_num = 1
JOIN customers_contact
    ON customers_with_onprem_subs.stripe_charge_id = customers_contact.stripe_charge_id
    AND customers_contact.row_num = 1
JOIN customers_opportunity
    ON customers_with_onprem_subs.stripe_charge_id = customers_opportunity.stripe_charge_id
    AND customers_opportunity.row_num = 1
JOIN customers_oli
    ON customers_with_onprem_subs.stripe_charge_id = customers_oli.stripe_charge_id
    AND customers_oli.row_num = 1