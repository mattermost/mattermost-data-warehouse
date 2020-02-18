BEGIN;

WITH leap_years AS (
    SELECT util_dates.date AS date
    FROM orgm.util_dates
    WHERE util_dates.date::varchar LIKE '%-02-29'
    GROUP BY 1
), opportunitylineitems_impacted AS (
    SELECT
      opportunity.sfid AS opportunity_sfid,
      opportunitylineitem.sfid AS opportunitylineitem_sfid,
      MAX(CASE WHEN leap_years.date BETWEEN start_date__c::date AND end_date__c::date THEN 1 ELSE 0 END) AS crosses_leap_day
    FROM orgm.opportunity
    LEFT JOIN orgm.opportunitylineitem ON opportunity.sfid = opportunitylineitem.opportunityid
    LEFT JOIN leap_years ON 1 = 1
    GROUP BY opportunity_sfid, opportunitylineitem_sfid
), account_w_arr AS (
  SELECT
    account.sfid AS account_sfid,
  	SUM(365*(opportunitylineitem.totalprice)/(opportunitylineitem.end_date__c::date - opportunitylineitem.start_date__c::date + 1 - crosses_leap_day)) AS total_arr
  FROM orgm.opportunitylineitem
  LEFT JOIN opportunitylineitems_impacted ON opportunitylineitems_impacted.opportunitylineitem_sfid = opportunitylineitem.sfid
  LEFT JOIN orgm.opportunity ON opportunity.sfid = opportunitylineitem.opportunityid
  LEFT JOIN orgm.account ON account.sfid = opportunity.accountid
  WHERE opportunity.iswon
    AND opportunitylineitem.end_date__c::date-opportunitylineitem.start_date__c::date <> 0
    AND current_date >= opportunitylineitem.start_date__c::date
    AND current_date <= opportunitylineitem.end_date__c::date
  GROUP BY 1
), all_accounts_arr AS (
  SELECT
    account.sfid AS account_sfid,
    COALESCE(total_arr,0) AS total_arr
  FROM orgm.account
  LEFT JOIN account_w_arr ON account.sfid = account_w_arr.account_sfid
)
UPDATE orgm.account
    SET arr_current__c = all_accounts_arr.total_arr
FROM all_accounts_arr
WHERE all_accounts_arr.account_sfid = account.sfid
    AND (all_accounts_arr.total_arr <> account.arr_current__c OR account.arr_current__c ISNULL);

COMMIT;

