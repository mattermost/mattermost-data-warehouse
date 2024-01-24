{{config({
    "schema": "hightouch",
    "materialized": "view",
    "tags":["hourly", "blapi", "deprecated"]
  })
}}

WITH cloud_accounts_to_sync as (
    SELECT
        customers_with_cloud_subs.*,
        '0053p0000064nt8AAA' AS ownerid,
        COALESCE(customers_with_cloud_subs.domain, '{{ var('unknown_account_name') }}' ) AS account_name
    FROM {{ ref('customers_with_cloud_subs') }}
    LEFT JOIN {{ ref('account') }}
        ON customers_with_cloud_subs.domain = account.cbit__clearbitdomain__c
            OR customers_with_cloud_subs.account_external_id = account.dwh_external_id__c
    LEFT JOIN {{ source('orgm', 'account_domain_mapping') }}
        ON customers_with_cloud_subs.domain = account_domain_mapping.domain
    WHERE account.id IS NULL AND account_domain_mapping.accountid IS NULL
    AND customers_with_cloud_subs.hightouch_sync_eligible
)
SELECT * FROM cloud_accounts_to_sync