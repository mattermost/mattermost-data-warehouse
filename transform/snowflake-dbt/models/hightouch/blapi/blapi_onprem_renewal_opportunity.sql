{{config({
    "schema": "hightouch",
    "materialized": "view",
    "tags":["hourly","blapi"]
  })
}}

WITH onprem_opportunities_to_sync AS (
    SELECT
        customers_with_onprem_subs.*,
        '6. Closed Won' as stagename,
        'Online' as order_type
    FROM {{ ref('customers_with_onprem_subs') }}
    JOIN {{ ref('opportunity') }}
        ON customers_with_onprem_subs.sfdc_migrated_opportunity_sfid = opportunity.id
    WHERE customers_with_onprem_subs.hightouch_sync_eligible
)
SELECT * FROM onprem_opportunities_to_sync