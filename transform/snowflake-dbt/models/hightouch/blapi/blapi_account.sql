{{config({
    "schema": "hightouch",
    "materialized": "view",
    "tags":["hourly","blapi", "deprecated"]
  })
}}

WITH onprem_accounts_to_sync as (
    SELECT
        customers_with_onprem_subs.*,
        '0053p0000064nt8AAA' AS ownerid
    FROM {{ ref('customers_with_onprem_subs') }}
    LEFT JOIN {{ ref('account') }}
        ON customers_with_onprem_subs.domain = account.cbit__clearbitdomain__c
            OR customers_with_onprem_subs.account_external_id = account.dwh_external_id__c
    LEFT JOIN {{ source('orgm', 'account_domain_mapping') }}
        ON customers_with_onprem_subs.domain = account_domain_mapping.domain
    WHERE account.id IS NULL AND account_domain_mapping.accountid IS NULL
    AND customers_with_onprem_subs.hightouch_sync_eligible
)
SELECT * FROM onprem_accounts_to_sync