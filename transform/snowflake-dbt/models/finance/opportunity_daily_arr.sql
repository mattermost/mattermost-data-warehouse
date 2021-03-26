{{config({
    "materialized": "table",
    "schema": "finance"
  })
}}

WITH leap_years AS (
    SELECT dates.date
    FROM {{ source('util', 'dates') }}
    WHERE dates.date LIKE '%-02-29'
    GROUP BY 1
), opportunitylineitems_impacted AS (
    SELECT
      opportunity.sfid AS opportunity_sfid,
      opportunitylineitem.sfid AS opportunitylineitem_sfid,
      MAX(CASE WHEN leap_years.date BETWEEN start_date__c::date AND end_date__c::date THEN 1 ELSE 0 END) AS crosses_leap_day
    FROM {{ ref( 'opportunity') }} AS opportunity
    LEFT JOIN {{ ref( 'opportunitylineitem') }} AS opportunitylineitem ON opportunity.sfid = opportunitylineitem.opportunityid
    LEFT JOIN leap_years ON 1 = 1
    GROUP BY opportunity_sfid, opportunitylineitem_sfid
), opportunity_daily_arr AS (
  SELECT
    opportunity.sfid AS opportunity_sfid,
    account.sfid AS account_sfid,
    coalesce(master_account.sfid, account.sfid) AS master_account_sfid,
  	util_dates.date::date AS day,
  	SUM(365*(opportunitylineitem.totalprice)/(opportunitylineitem.end_date__c::date - opportunitylineitem.start_date__c::date + 1 - crosses_leap_day))::int AS total_arr
  FROM {{ ref( 'opportunitylineitem') }}  AS opportunitylineitem
  LEFT JOIN opportunitylineitems_impacted ON opportunitylineitems_impacted.opportunitylineitem_sfid = opportunitylineitem.sfid
  LEFT JOIN {{ ref( 'opportunity') }}  AS opportunity ON opportunity.sfid = opportunitylineitem.opportunityid
  LEFT JOIN {{ ref( 'account') }}  AS account ON account.sfid = opportunity.accountid
  LEFT JOIN {{ ref( 'account') }}  AS master_account ON master_account.sfid = account.parentid
  LEFT JOIN {{ source('util', 'dates') }}  AS util_dates ON util_dates.date::date >= opportunitylineitem.start_date__c::date AND util_dates.date::date <= opportunitylineitem.end_date__c::date
  WHERE opportunity.iswon
    AND opportunitylineitem.end_date__c::date-opportunitylineitem.start_date__c::date <> 0 AND opportunitylineitem.product_type__c = 'Recurring'
  GROUP BY 1, 2, 3, 4
)
SELECT * FROM opportunity_daily_arr