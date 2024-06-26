{{config({
    "materialized": "table",
    "schema": "finance",
    "tags":["nightly"]
  })
}}

WITH account_dates AS (
    SELECT 
      account.sfid AS account_sfid, 
      coalesce(master_account.sfid, account.sfid) AS master_account_sfid,
      min(start_date__c) AS min_start_date,
      max(end_date__c) AS max_end_date
    FROM orgm.account
        LEFT JOIN {{ ref( 'account') }} AS master_account ON master_account.sfid = account.parentid
        JOIN {{ ref( 'opportunity') }} ON opportunity.accountid = account.sfid
        JOIN {{ ref( 'opportunitylineitem') }} ON opportunity.sfid = opportunitylineitem.opportunityid
    WHERE opportunity.iswon
    GROUP BY 1, 2
), with_util_dates AS (
    SELECT
        dates.date AS day,
        account_sfid,
        master_account_sfid
    FROM {{ ref('dates') }}
    JOIN account_dates AS account ON 1 = 1
    WHERE dates.date >= min_start_date - interval '1 day' AND dates.date <= max_end_date + interval '2 day'
)
SELECT * FROM with_util_dates
