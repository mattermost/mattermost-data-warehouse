{{ config
    ({
        "materialized": 'table',
        "schema": "int_hightouch",
        "unique_key":['id'],
  })
}}

WITH latest_credit_card_address AS (
    SELECT
        invoices.customer,
        invoices.line1 as line1,
        invoices.line2 as line2,
        invoices.postal_code as postal_code,
        invoices.city as city,
        invoices.state as state,
        invoices.country as country,
        invoices.state as state_code,
        invoices.country as country_code,
        ROW_NUMBER() OVER (PARTITION BY invoices.customer ORDER BY invoices.created_at DESC) as row_num
    FROM {{ ref('stg_stripe__invoices') }} invoices
        )
        , customers_with_onprem_subs AS (
    SELECT
        customers.customer_id as customer_id,
        customers.email,
        customers.contact_first_name as first_name,
        customers.contact_last_name as last_name,
        SPLIT_PART(customers.email, '@', 2) as domain,
        onprem_subscriptions.subscription_id as subscription_id,
        CASE 
            WHEN renewed_from_subscription.subscription_id is not null 
            THEN coalesce(renewed_from_subscription.stripe_charge_id, lag(onprem_subscriptions.stripe_charge_id,1) 
            over (partition by customers.customer_id order by onprem_subscriptions.created_at))
            ELSE NULL
        END as previous_stripe_charge_id,
        to_varchar(onprem_subscriptions.current_period_start_at, 'yyyy-mm-dd"T"hh24:mi:ss"Z"') as start_date,
        to_varchar(onprem_subscriptions.current_period_end_at, 'yyyy-mm-dd"T"hh24:mi:ss"Z"') as end_date,
        CASE 
            WHEN subscriptions.renewed_from_subscription_id is not null 
            THEN invoices.total / 100.0
            ELSE onprem_subscriptions.invoice_amount / 100.0 
        END as total,
        onprem_subscriptions.invoice_amount / 100.0 as listed_total,
        to_varchar(onprem_subscriptions.updated_at, 'yyyy-mm-dd"T"hh24:mi:ss"Z"') as updated_at,
        onprem_subscriptions.invoice_number,
        onprem_subscriptions.stripe_charge_id,
        onprem_subscriptions.invoice_quantity,
        onprem_subscriptions.sku as sku,
        --products.pricebookentryid,
        to_varchar(subscriptions.actual_renewal_at, 'yyyy-mm-dd"T"hh24:mi:ss"Z"') as actual_renewal_at,
        dateadd(day, 1, subscriptions.actual_renewal_at::date) as renewal_start_date,
        dateadd(year, 1, subscriptions.actual_renewal_at::date) as renewal_end_date,
        renewed_from_subscription.sfdc_migrated_opportunity_sfid,
        CASE 
            WHEN renewed_from_subscription.subscription_id is not null 
            THEN true
            ELSE false
        END as is_renewed,
        coalesce(
        CASE 
            WHEN subscriptions.renewed_from_subscription_id is not null 
            THEN invoices.total
            ELSE onprem_subscriptions.invoice_amount
            END
            , 0) / 100.0 as renewed_from_total,
        COALESCE(subscriptions.license_id, onprem_subscriptions.subscription_id) as license_key,
        subscriptions.purchase_order_number,
        latest_credit_card_address.line1,
        latest_credit_card_address.line2,
        latest_credit_card_address.line1 || ' ' || coalesce(latest_credit_card_address.line2, '') as street_address,
        latest_credit_card_address.postal_code,
        latest_credit_card_address.city,
        latest_credit_card_address.state,
        latest_credit_card_address.country,
        latest_credit_card_address.state_code,
        latest_credit_card_address.country_code,
        onprem_subscriptions.updated_at >= '2021-08-18' as hightouch_sync_eligible
    FROM {{ ref('stg_stripe__customers') }} customers
        JOIN {{ ref('int_stripe_onprem_subscriptions') }} onprem_subscriptions ON customers.customer_id = onprem_subscriptions.stripe_customer_id
        JOIN {{ ref('stg_stripe__subscriptions') }} subscriptions ON onprem_subscriptions.subscription_id = subscriptions.subscription_id
        --JOIN {{ source('blapi','products') }} products ON onprem_subscriptions.sku = products.sku
        LEFT JOIN {{ ref('stg_stripe__subscriptions') }} renewed_subscriptions
            ON subscriptions.subscription_id = renewed_subscriptions.renewed_from_subscription_id
        LEFT JOIN {{ ref('int_stripe_onprem_subscriptions') }} renewed_from_subscription ON subscriptions.renewed_from_subscription_id = renewed_from_subscription.subscription_id
        LEFT JOIN {{ ref('stg_stripe__invoices') }} invoices ON onprem_subscriptions.stripe_invoice_number = invoices.number 
        JOIN latest_credit_card_address
            ON customers.customer_id = latest_credit_card_address.customer
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
        account.account_id as account_id,
        account.type as account_type,
        ROW_NUMBER() OVER (PARTITION BY customers_with_onprem_subs.stripe_charge_id ORDER BY account.last_modified_at DESC) as row_num
    FROM customers_with_onprem_subs
    LEFT JOIN {{ ref('stg_salesforce__account') }} account
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
        contact.contact_id as contact_sfid,
        account.account_id as account_id,
        account.dwh_external_id__c as contact_account_external_id,
        ROW_NUMBER() OVER (PARTITION BY customers_with_onprem_subs.stripe_charge_id ORDER BY contact.last_modified_at DESC) as row_num
    FROM customers_with_onprem_subs
    LEFT JOIN {{ ref('stg_salesforce__contact') }} contact
       ON customers_with_onprem_subs.email = contact.email
    LEFT JOIN {{ ref('stg_salesforce__account') }} account
        ON contact.account_id = account.account_id
), customers_opportunity AS (
    SELECT
        customers_with_onprem_subs.stripe_charge_id,
            opportunity.dwh_external_id__c as opportunity_external_id,
        opportunity.opportunity_id as opportunity_sfid,
        ROW_NUMBER() OVER (PARTITION BY customers_with_onprem_subs.stripe_charge_id ORDER BY opportunity.last_modified_at DESC) as row_num
    FROM customers_with_onprem_subs
    LEFT JOIN {{ ref('stg_salesforce__opportunity') }} opportunity
        ON customers_with_onprem_subs.stripe_charge_id = opportunity.stripe_id__c
),  customers_previous_opportunity AS (
    SELECT 
        customers_with_onprem_subs.subscription_id, 
        opportunity.opportunity_id as previous_opportunity_sfid,
        opportunity.amount as up_for_renewal_arr,
        ROW_NUMBER() OVER (PARTITION BY customers_with_onprem_subs.stripe_charge_id ORDER BY opportunity.last_modified_at DESC) as row_num
    FROM customers_with_onprem_subs
    JOIN {{ ref('stg_salesforce__opportunity') }} opportunity
        ON customers_with_onprem_subs.previous_stripe_charge_id = opportunity.stripe_id__c
        WHERE customers_with_onprem_subs.is_renewed

), customers_oli AS (
    SELECT
        customers_with_onprem_subs.stripe_charge_id,
        COALESCE(
            opportunitylineitem.dwh_external_id__c,
            UUID_STRING('78157189-82de-4f4d-9db3-88c601fbc22e', customers_opportunity.opportunity_external_id || 'oli')
        ) AS opportunitylineitem_external_id,
        opportunitylineitem.opportunity_line_item as opportunitylineitem_sfid,
        ROW_NUMBER() OVER (PARTITION BY customers_with_onprem_subs.stripe_charge_id ORDER BY opportunitylineitem.last_modified_at DESC) as row_num
    FROM customers_with_onprem_subs
    LEFT JOIN customers_opportunity
        ON customers_with_onprem_subs.stripe_charge_id = customers_opportunity.stripe_charge_id
    LEFT JOIN {{ ref('stg_salesforce__opportunity_line_item') }} opportunitylineitem
        ON customers_opportunity.opportunity_sfid = opportunitylineitem.opportunity_id
)
SELECT
    customers_with_onprem_subs.*,
    case
        when customers_account.account_id is null and customers_contact.account_id is not null
        then customers_contact.contact_account_external_id
        else customers_account.account_external_id
    end as account_external_id,
    customers_account.account_type,
    coalesce(customers_account.account_id, customers_contact.account_id) as account_id,
    customers_contact.contact_external_id,
    customers_contact.contact_sfid,
    customers_opportunity.opportunity_external_id,
    customers_opportunity.opportunity_sfid,
    customers_oli.opportunitylineitem_external_id,
    customers_oli.opportunitylineitem_sfid,
    customers_previous_opportunity.previous_opportunity_sfid,
    customers_previous_opportunity.up_for_renewal_arr
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
LEFT JOIN customers_previous_opportunity
    ON customers_with_onprem_subs.subscription_id = customers_previous_opportunity.subscription_id
    AND customers_previous_opportunity.row_num = 1