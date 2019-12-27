{{config({
    "materialized": "table",
    "schema": "finance"
  })
}}

WITH account_daily_arr AS (
  SELECT
    account.sfid as account_sfid,
    coalesce(parent_account.sfid, account.sfid) as master_account_sfid,
  	util_dates.date::date AS day,
    SUM(opportunitylineitem.totalprice)::int AS total_arr,
  	SUM(365*(opportunitylineitem.totalprice)/(opportunitylineitem.end_date__c::date-opportunitylineitem.start_date__c::date))::int AS total_arr_norm
  FROM {{ source('orgm', 'opportunitylineitem') }}  AS opportunitylineitem
  LEFT JOIN {{ source('orgm', 'opportunity') }}  AS opportunity ON opportunity.sfid = opportunitylineitem.opportunityid
  LEFT JOIN {{ source('orgm', 'account') }}  AS account ON account.sfid = opportunity.accountid
  LEFT JOIN {{ source('orgm', 'account') }}  AS parent_account ON parent_account.sfid = account.parentid
  LEFT JOIN {{ source('util', 'dates') }}  AS util_dates ON util_dates.date::date >= opportunitylineitem.start_date__c::date and util_dates.date::date <= opportunitylineitem.end_date__c::date
  WHERE opportunity.iswon
    AND opportunitylineitem.end_date__c::date-opportunitylineitem.start_date__c::date <> 0
  GROUP BY 1, 2, 3
)
SELECT * FROM account_daily_arr