{{config({
    "schema": "int_hightouch",
    "unique_key":['id'],
  })
}}

WITH latest_credit_card_address AS (
    SELECT
        invoices.customer,
        TRIM(customer_shipping:"address":"line1",'"') as line1,
        TRIM(customer_shipping:"address":"line2",'"') as line2,
        TRIM(customer_shipping:"address":"postal_code",'"') as postal_code,
        TRIM(customer_shipping:"address":"city",'"') as city,
        TRIM(customer_shipping:"address":"state",'"') as state,
        TRIM(customer_shipping:"address":"country",'"') as country,
        state as state_code,
        country as country_code,
        ROW_NUMBER() OVER (PARTITION BY invoices.customer ORDER BY invoices.created DESC) as row_num
    FROM {{ source('stripe_raw','invoices') }} invoices
        )
        , customers_with_onprem_subs AS (
    SELECT
        customers.id as customer_id,
        customers.email,
        TRIM(customers.metadata:"cws-additional-contact-first-name",'"') as first_name,
        TRIM(customers.metadata:"cws-additional-contact-last-name",'"') as last_name,
        SPLIT_PART(customers.email, '@', 2) as domain,
        onprem_subscriptions.id as subscription_id,
        CASE 
            WHEN renewed_from_subscription.id is not null 
            THEN coalesce(renewed_from_blapi_subscription.stripe_charge_id, lag(onprem_subscriptions.stripe_charge_id,1) 
            over (partition by customers.id order by onprem_subscriptions.created))
            ELSE NULL
        END as previous_stripe_charge_id,
        to_varchar(onprem_subscriptions.current_period_start, 'yyyy-mm-dd"T"hh24:mi:ss"Z"') as start_date,
        to_varchar(onprem_subscriptions.current_period_end, 'yyyy-mm-dd"T"hh24:mi:ss"Z"') as end_date,
        CASE 
            WHEN subscriptions.renewed_from_sub_id is not null 
            THEN invoices.total / 100.0
            ELSE onprem_subscriptions.invoice_amount / 100.0 
        END as total,
        onprem_subscriptions.invoice_amount / 100.0 as listed_total,
        to_varchar(onprem_subscriptions.updated, 'yyyy-mm-dd"T"hh24:mi:ss"Z"') as updated_at,
        onprem_subscriptions.invoice_number,
        onprem_subscriptions.stripe_charge_id,
        onprem_subscriptions.invoice_quantity,
        onprem_subscriptions.sku as sku,
        products.pricebookentryid,
        to_varchar(subscriptions.actual_renewal_date, 'yyyy-mm-dd"T"hh24:mi:ss"Z"') as actual_renewal_date,
        dateadd(day, 1, subscriptions.actual_renewal_date::date) as renewal_start_date,
        dateadd(year, 1, subscriptions.actual_renewal_date::date) as renewal_end_date,
        renewed_from_subscription.sfdc_migrated_opportunity_sfid,
        CASE 
            WHEN renewed_from_subscription.id is not null 
            THEN true
            ELSE false
        END as is_renewed,
        coalesce(
        CASE 
            WHEN subscriptions.renewed_from_sub_id is not null 
            THEN invoices.total
            ELSE onprem_subscriptions.invoice_amount
            END
            , 0) / 100.0 as renewed_from_total,
        COALESCE(subscriptions.license_id, onprem_subscriptions.id) as license_key,
        subscriptions.purchase_order_num,
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
        onprem_subscriptions.updated >= '2021-08-18' as hightouch_sync_eligible
    FROM {{ source('stripe_raw','customers') }} customers
        JOIN {{ ref('stg_stripe_onprem_subscriptions') }} ON customers.id = onprem_subscriptions.customer
        JOIN "ANALYTICS".stripe.subscriptions ON onprem_subscriptions.id = subscriptions.id
        JOIN {{ source('blapi','products') }} ON onprem_subscriptions.sku = products.sku
        LEFT JOIN {{ source('stripe_raw','subscriptions') }} renewed_from_subscription
            ON subscriptions.renewed_from_sub_id = renewed_from_subscription.id
        LEFT JOIN {{ ref('stg_stripe_onprem_subscriptions') }} renewed_from_blapi_subscription ON subscriptions.renewed_from_sub_id = renewed_from_blapi_subscription.id
        LEFT JOIN {{ source('stripe_raw','invoices') }} ON onprem_subscriptions.stripe_invoice_number = invoices.number 
        JOIN latest_credit_card_address
            ON customers.id = latest_credit_card_address.customer
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
        account.type as account_type,
        ROW_NUMBER() OVER (PARTITION BY customers_with_onprem_subs.stripe_charge_id ORDER BY account.lastmodifieddate DESC) as row_num
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
        account.id as account_sfid,
        account.dwh_external_id__c as contact_account_external_id,
        ROW_NUMBER() OVER (PARTITION BY customers_with_onprem_subs.stripe_charge_id ORDER BY contact.lastmodifieddate DESC) as row_num
    FROM customers_with_onprem_subs
    LEFT JOIN {{ ref('contact') }}
       ON customers_with_onprem_subs.email = contact.email
    LEFT JOIN {{ ref('account') }}
        ON contact.accountid = account.id
), customers_opportunity AS (
    SELECT
        customers_with_onprem_subs.stripe_charge_id,
            opportunity.dwh_external_id__c as opportunity_external_id,
        opportunity.sfid as opportunity_sfid,
        ROW_NUMBER() OVER (PARTITION BY customers_with_onprem_subs.stripe_charge_id ORDER BY opportunity.lastmodifieddate DESC) as row_num
    FROM customers_with_onprem_subs
    LEFT JOIN {{ ref('opportunity') }}
        ON customers_with_onprem_subs.stripe_charge_id = opportunity.stripe_id__c
),  customers_previous_opportunity AS (
    SELECT 
        customers_with_onprem_subs.subscription_id, 
        opportunity.sfid as previous_opportunity_sfid,
        opportunity.amount as up_for_renewal_arr,
        ROW_NUMBER() OVER (PARTITION BY customers_with_onprem_subs.stripe_charge_id ORDER BY opportunity.lastmodifieddate DESC) as row_num
    FROM customers_with_onprem_subs
    JOIN {{ ref('opportunity') }}
        ON customers_with_onprem_subs.previous_stripe_charge_id = opportunity.stripe_id__c
        WHERE customers_with_onprem_subs.is_renewed

), customers_oli AS (
    SELECT
        customers_with_onprem_subs.stripe_charge_id,
        COALESCE(
            opportunitylineitem.dwh_external_id__c,
            UUID_STRING('78157189-82de-4f4d-9db3-88c601fbc22e', customers_opportunity.opportunity_external_id || 'oli')
        ) AS opportunitylineitem_external_id,
        opportunitylineitem.sfid as opportunitylineitem_sfid,
        ROW_NUMBER() OVER (PARTITION BY customers_with_onprem_subs.stripe_charge_id ORDER BY opportunitylineitem.lastmodifieddate DESC) as row_num
    FROM customers_with_onprem_subs
    LEFT JOIN customers_opportunity
        ON customers_with_onprem_subs.stripe_charge_id = customers_opportunity.stripe_charge_id
    LEFT JOIN {{ ref('opportunitylineitem') }}
        ON customers_opportunity.opportunity_sfid = opportunitylineitem.opportunityid
)
SELECT
    customers_with_onprem_subs.*,
    case
        when customers_account.account_sfid is null and customers_contact.account_sfid is not null
        then customers_contact.contact_account_external_id
        else customers_account.account_external_id
    end as account_external_id,
    customers_account.account_type,
    coalesce(customers_account.account_sfid, customers_contact.account_sfid) as account_sfid,
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