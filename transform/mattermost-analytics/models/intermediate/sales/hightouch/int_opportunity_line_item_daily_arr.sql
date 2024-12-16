{{
  config({
    "materialized": "table",
  })
}}

WITH leap_years AS (
    SELECT dates.date
    FROM {{ ref('arr_days') }}
    WHERE dates.date LIKE '%-02-29'
    GROUP BY 1
), opportunitylineitems_impacted AS (
    SELECT
      opportunity_line_item_id,
      MAX(CASE WHEN leap_years.date BETWEEN start_date__c::date AND end_date__c::date THEN 1 ELSE 0 END) AS crosses_leap_day
    FROM {{ ref( 'stg_salesforce__opportunity_line_item') }}
    LEFT JOIN leap_years ON 1 = 1
    GROUP BY opportunitylineitem_sfid
)
SELECT
    opportunitylineitem.opportunity_line_item_id,
    opportunity.opportunity_id,
  	util_dates.dates_day::date AS day,
  	SUM(CASE WHEN opportunity.is_won THEN 365*(opportunitylineitem.totalprice)/(opportunitylineitem.end_date__c::date - opportunitylineitem.start_date__c::date + 1 - crosses_leap_day) ELSE 0 END )::int AS won_arr,
    SUM(CASE WHEN opportunity.is_closed AND NOT opportunity.iswon THEN 365*(opportunitylineitem.totalprice)/(opportunitylineitem.end_date__c::date - opportunitylineitem.start_date__c::date + 1 - crosses_leap_day)ELSE 0 END )::int AS lost_arr,
    SUM(CASE WHEN NOT opportunity.is_closed THEN 365*(opportunitylineitem.totalprice)/(opportunitylineitem.end_date__c::date - opportunitylineitem.start_date__c::date + 1 - crosses_leap_day) ELSE 0 END )::int AS open_arr
FROM
    {{ ref( 'stg_salesforce__opportunity_line_item') }}  AS opportunitylineitem
    LEFT JOIN opportunitylineitems_impacted ON opportunitylineitems_impacted.opportunitylineitem_sfid = opportunitylineitem.sfid
    LEFT JOIN {{ ref( 'stg_salesforce__opportunity') }}  AS opportunity ON opportunity.opportunity_id = opportunitylineitem.opportunity_id
    LEFT JOIN {{ ref('arr_dates') }}  AS util_dates ON util_dates.date_day::date >= opportunitylineitem.start_date__c::date AND util_dates.date::date <= opportunitylineitem.end_date__c::date
WHERE
    opportunitylineitem.end_date__c::date-opportunitylineitem.start_date__c::date <> 0
    AND opportunitylineitem.end_date__c::date - opportunitylineitem.start_date__c::date + 1 - crosses_leap_day <> 0
    AND opportunitylineitem.product_type__c = 'Recurring'
GROUP BY 1, 2, 3
