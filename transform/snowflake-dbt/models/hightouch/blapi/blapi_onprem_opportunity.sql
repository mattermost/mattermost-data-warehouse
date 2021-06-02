{{config({
    "schema": "hightouch",
    "materialized": "view",
    "tags":["hourly","blapi"]
  })
}}

WITH onprem_opportunities_to_sync AS (
    SELECT
        customers_with_onprem_subs.*,
        'New Subscription' AS opportunity_type,
        '6. Closed Won' as stagename,
        CASE WHEN account.name = 'Hold Public'
            THEN
                customers_with_onprem_subs.email || ' ' ||
                customers_with_onprem_subs.sku || ' qty:' ||
                customers_with_onprem_subs.num_seats || ' inv:' ||
                customers_with_onprem_subs.invoice_number
            ELSE
                account.name || ' ' ||
                customers_with_onprem_subs.sku || ' qty:' ||
                customers_with_onprem_subs.num_seats || ' inv:' ||
                customers_with_onprem_subs.invoice_number
        END AS opportunity_name
    FROM {{ ref('customers_with_onprem_subs') }}
    LEFT JOIN {{ ref('opportunity') }}
        ON customers_with_onprem_subs.opportunity_external_id = opportunity.dwh_external_id__c
            OR customers_with_onprem_subs.stripe_charge_id = opportunity.stripe_id__c
    LEFT JOIN {{ ref('account') }}
        ON opportunity.accountid = account.id
    WHERE opportunity.id IS NULL
)
SELECT * FROM onprem_opportunities_to_sync