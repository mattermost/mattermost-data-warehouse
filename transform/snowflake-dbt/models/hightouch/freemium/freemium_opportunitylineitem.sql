{{config({
    "schema": "hightouch",
    "materialized": "view",
    "tags":["hourly","blapi"]
  })
}}

WITH freemium_opportunitylineitems_to_sync AS (
    SELECT DISTINCT
        customers_with_freemium_subs.*,
        opportunitylineitem.sfid as opportunitylineitem_sfid,
        COALESCE(
            opportunitylineitem.dwh_external_id__c,
            UUID_STRING('78157189-82de-4f4d-9db3-88c601fbc22e', customers_with_freemium_subs.invoice_number)
        ) AS opportunitylineitem_external_id,
        '6. Closed Won' as stagename,
        '01t3p00000RcfjJAAR' as product_id,
        'New Subscription' as opportunity_type,
        'Monthly Billing' as product_type,
        'New' as product_line_type,
        1 as quantity,
        'Discount' as pricing_method,
        0 as discount_calc,
        false as sales_price_needs_to_be_updated
    FROM {{ ref('customers_with_freemium_subs') }}
    LEFT JOIN {{ ref('opportunity') }} 
        ON customers_with_freemium_subs.opportunity_external_id = opportunity.dwh_external_id__c
    LEFT JOIN {{ ref('opportunitylineitem') }} 
        ON opportunitylineitem.opportunityid = opportunity.sfid
    WHERE customers_with_freemium_subs.hightouch_sync_eligible
    AND customers_with_freemium_subs.sku = 'Cloud Professional'
)
SELECT * FROM freemium_opportunitylineitems_to_sync