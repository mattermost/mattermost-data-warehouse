{{config({
    "materialized": "table",
    "schema": "finance"
  })
}}

WITH account_dates AS (
  SELECT account.sfid AS account_sfid, coalesce(parent_account.sfid, account.sfid) AS mASter_account_sfid, min(start_date__c) AS min_start_date, max(end_date__c) AS max_end_date
    FROM orgm.account
        LEFT JOIN {{ source('orgm', 'account') }} AS parent_account ON parent_account.sfid = account.parentid
        JOIN {{ source('orgm', 'opportunity') }} ON opportunity.accountid = account.sfid
        JOIN {{ source('orgm', 'opportunitylineitem') }} ON opportunity.sfid = opportunitylineitem.opportunityid
    WHERE opportunity.iswon
    GROUP BY 1, 2
), with_util_dates AS (
    SELECT
        dates.date AS day,
        account_sfid,
        mASter_account_sfid
    FROM {{ source('util', 'dates') }}
    JOIN account_dates AS account ON 1 = 1
    WHERE dates.date >= min_start_date - interval '1 day' AND dates.date <= max_end_date + interval '2 day'
)
SELECT * FROM with_util_dates
