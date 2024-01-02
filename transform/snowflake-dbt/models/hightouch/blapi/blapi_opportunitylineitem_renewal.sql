{{config({
    "schema": "hightouch",
    "materialized": "view",
    "tags":["hourly", "blapi", "deprecated"]
  })
}}

WITH onprem_olis_to_sync AS (
    SELECT
        customers_with_onprem_subs.*,
        'Recurring' AS product_type,
        'Mixed' AS product_line_type,
        'Discount' AS pricing_method,
        customers_with_onprem_subs.listed_total/customers_with_onprem_subs.num_seats as list_price,
        customers_with_onprem_subs.listed_total - customers_with_onprem_subs.total as discount,
        (customers_with_onprem_subs.listed_total - customers_with_onprem_subs.total)/customers_with_onprem_subs.listed_total as discount_percent,
        customers_with_onprem_subs.listed_total/customers_with_onprem_subs.num_seats * (1 - (customers_with_onprem_subs.listed_total - customers_with_onprem_subs.total)/customers_with_onprem_subs.listed_total) as unit_price,
        0 AS zero_amount,
        customers_with_onprem_subs.total - customers_with_onprem_subs.renewed_from_total as expansion_amount
    FROM {{ ref('customers_with_onprem_subs') }}
    LEFT JOIN {{ ref('opportunitylineitem') }}
        ON customers_with_onprem_subs.subscription_version_id = opportunitylineitem.subs_version_id__c
    WHERE opportunitylineitem.id IS NULL
    -- Same filters as in blapi_onprem_opportunity_renewal
    AND customers_with_onprem_subs.is_renewed
    AND customers_with_onprem_subs.account_sfid is not null
    AND customers_with_onprem_subs.account_type in ('Customer','Customer (Attrited)')
    AND customers_with_onprem_subs.opportunity_sfid is not null
    AND customers_with_onprem_subs.hightouch_sync_eligible
)
SELECT * FROM onprem_olis_to_sync
WHERE
    subscription_version_id not in (
        '8kis7psj1jb5pqifnsr8dxf3ie'   -- See https://mattermost.atlassian.net/browse/MM-52880
    )
