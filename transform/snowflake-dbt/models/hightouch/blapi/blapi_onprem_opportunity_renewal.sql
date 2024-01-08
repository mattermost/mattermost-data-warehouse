{{config({
    "schema": "hightouch",
    "materialized": "view",
    "tags":["hourly", "blapi", "deprecated"]
  })
}}

WITH onprem_opportunities_to_sync AS (
    SELECT
        customers_with_onprem_subs.*,
        'Renewal' AS opportunity_type,
        'Online' as order_type,
        '0053p0000064nt8AAA' AS ownerid,
        '6. Closed Won' as stagename,
        CASE WHEN account.name = 'Hold Public'
            THEN
                customers_with_onprem_subs.email || ' ' ||
                customers_with_onprem_subs.sku || ' qty:' ||
                customers_with_onprem_subs.num_seats || ' inv:' ||
                customers_with_onprem_subs.invoice_number
            ELSE
                customers_with_onprem_subs.domain || ' ' ||
                customers_with_onprem_subs.sku || ' qty:' ||
                customers_with_onprem_subs.num_seats || ' inv:' ||
                customers_with_onprem_subs.invoice_number
        END AS opportunity_name
    FROM {{ ref('customers_with_onprem_subs') }}
    LEFT JOIN {{ ref('opportunity') }}
        ON customers_with_onprem_subs.opportunity_external_id = opportunity.dwh_external_id__c
            OR customers_with_onprem_subs.stripe_charge_id = opportunity.stripe_id__c
    LEFT JOIN {{ ref('account') }}
        ON customers_with_onprem_subs.account_external_id = account.dwh_external_id__c
    WHERE opportunity.id IS NULL
        AND customers_with_onprem_subs.is_renewed 
        AND customers_with_onprem_subs.account_sfid is not null 
        AND customers_with_onprem_subs.account_type in ('Customer','Customer (Attrited)')
        AND customers_with_onprem_subs.hightouch_sync_eligible
)
SELECT * FROM onprem_opportunities_to_sync