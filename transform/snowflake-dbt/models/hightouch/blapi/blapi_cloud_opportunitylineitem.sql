{{config({
    "schema": "hightouch",
    "materialized": "view",
    "tags":["hourly", "blapi", "deprecated"]
  })
}}

WITH oli_for_invoice as (
    SELECT
        opportunity.sfid as opportunity_sfid,
        opportunity.dwh_external_id__c as opportunity_external_id,
        opportunitylineitem.sfid as opportunitylineitem_sfid,
        opportunitylineitem.dwh_external_id__c as opportunitylineitem_external_id,
        opportunitylineitem.invoice_id__c as invoice_id
    FROM
        {{ ref('opportunitylineitem') }}
        JOIN {{ ref('opportunity') }}
            ON opportunitylineitem.opportunityid = opportunity.sfid
    WHERE opportunity.type = 'Monthly Billing'
), cloud_opportunitylineitems_to_sync AS (
    SELECT DISTINCT
        customers_with_cloud_subs.*,
        oli_for_invoice.opportunitylineitem_sfid,
        COALESCE(
            oli_for_invoice.opportunitylineitem_external_id,
            UUID_STRING('78157189-82de-4f4d-9db3-88c601fbc22e', invoices_blapi.id)
        ) AS opportunitylineitem_external_id,
        invoices_blapi.id as invoice_id,
        to_varchar(invoices_blapi.start_date, 'yyyy-mm-dd"T"hh24:mi:ss"Z"') as invoice_start_date,
        to_varchar(DATEADD(day, -1, invoices_blapi.end_date::date), 'yyyy-mm-dd"T"hh24:mi:ss"Z"') as invoice_end_date,
        0.01 as listprice,
        invoices_blapi.total as quantity,
        0 as discount_calc,
        'Discount' as pricing_method,
        round(invoices_blapi.total / 100.0, 2) as total_dollars,
        'Billing' as product_line_type,
        'Monthly Billing' AS product_type,
        false as false_boolean,
        '01u3p00000uaI3WAAU' as pricebookentryid,
        invoices.metadata:netsuite_invoice_id as netsuite_invoice_id,
        'Online' as order_type,
        '0053p0000064nt8AAA' AS ownerid,
        '6. Closed Won' as stagename,
        case
            when invoices.status in ('draft', 'open', 'upcoming')
            then 'Pending'
            when invoices.status != 'paid'
            then 'Declined'
            when invoices.status = 'paid'
            then 'Paid'
            else 'Pending'
        end as invoice_status
    FROM {{ ref('customers_with_cloud_subs') }}
    JOIN {{ ref('invoices_blapi') }} invoices_blapi
        ON customers_with_cloud_subs.subscription_id = invoices_blapi.subscription_id
    LEFT JOIN {{ source('stripe_raw', 'invoices') }}
        ON invoices_blapi.stripe_id = invoices.id
    LEFT JOIN oli_for_invoice
        ON invoices_blapi.id = oli_for_invoice.invoice_id
    LEFT JOIN {{ ref('opportunity') }}
        ON oli_for_invoice.opportunity_external_id = opportunity.dwh_external_id__c
    WHERE customers_with_cloud_subs.hightouch_sync_eligible
    AND invoices_blapi.stripe_id != 'upcoming'
    AND invoices_blapi.total > 0
)
SELECT * FROM cloud_opportunitylineitems_to_sync