{{config({
    "materialized": "table",
    "schema": "finance",
    "tags":["nightly"]
  })
}}

WITH leap_years AS (
    SELECT dates.date
    FROM {{ ref('dates') }}
    WHERE dates.date LIKE '%-02-29'
    GROUP BY 1
), opportunitylineitems_impacted AS (
    SELECT
      opportunitylineitem.sfid AS opportunitylineitem_sfid,
      MAX(CASE WHEN leap_years.date BETWEEN start_date__c::date AND end_date__c::date THEN 1 ELSE 0 END) AS crosses_leap_day
    FROM {{ ref( 'opportunitylineitem') }}
    LEFT JOIN leap_years ON 1 = 1
    GROUP BY opportunitylineitem_sfid
), opportunitylineitem_daily_arr AS (
  SELECT
    opportunitylineitem.sfid as opportunitylineitem_sfid,
    opportunity.sfid AS opportunity_sfid,
  	util_dates.date::date AS day,
  	SUM(CASE WHEN opportunity.iswon THEN 365*(opportunitylineitem.totalprice)/(opportunitylineitem.end_date__c::date - opportunitylineitem.start_date__c::date + 1 - crosses_leap_day) ELSE 0 END )::int AS won_arr,
    SUM(CASE WHEN opportunity.isclosed AND NOT opportunity.iswon THEN 365*(opportunitylineitem.totalprice)/(opportunitylineitem.end_date__c::date - opportunitylineitem.start_date__c::date + 1 - crosses_leap_day)ELSE 0 END )::int AS lost_arr,
    SUM(CASE WHEN NOT opportunity.isclosed THEN 365*(opportunitylineitem.totalprice)/(opportunitylineitem.end_date__c::date - opportunitylineitem.start_date__c::date + 1 - crosses_leap_day) ELSE 0 END )::int AS open_arr,
    SUM(CASE WHEN opportunity.iswon THEN 365*(opportunitylineitem.new_amount__c)/(opportunitylineitem.end_date__c::date - opportunitylineitem.start_date__c::date + 1 - crosses_leap_day) ELSE 0 END )::int AS won_new_arr,
    SUM(CASE WHEN opportunity.iswon THEN 365*(opportunitylineitem.expansion_amount__c)/(opportunitylineitem.end_date__c::date - opportunitylineitem.start_date__c::date + 1 - crosses_leap_day) ELSE 0 END )::int AS won_expansion_arr,
    SUM(CASE WHEN opportunity.iswon THEN 365*(opportunitylineitem.coterm_expansion_amount__c)/(opportunitylineitem.end_date__c::date - opportunitylineitem.start_date__c::date + 1 - crosses_leap_day) ELSE 0 END )::int AS won_coterm_expansion_arr,
    SUM(CASE WHEN opportunity.iswon THEN 365*(opportunitylineitem.leftover_expansion_amount__c)/(opportunitylineitem.end_date__c::date - opportunitylineitem.start_date__c::date + 1 - crosses_leap_day) ELSE 0 END )::int AS won_leftover_expansion_arr,
    SUM(CASE WHEN opportunity.iswon THEN 365*(opportunitylineitem.renewal_amount__c)/(opportunitylineitem.end_date__c::date - opportunitylineitem.start_date__c::date + 1 - crosses_leap_day) ELSE 0 END )::int AS won_renewal_arr,
    SUM(CASE WHEN opportunity.iswon THEN 365*(opportunitylineitem.multi_amount__c)/(opportunitylineitem.end_date__c::date - opportunitylineitem.start_date__c::date + 1 - crosses_leap_day) ELSE 0 END )::int AS won_multi_arr,
    SUM(CASE WHEN opportunity.iswon THEN 365*(opportunitylineitem.renewal_multi_amount__c)/(opportunitylineitem.end_date__c::date - opportunitylineitem.start_date__c::date + 1 - crosses_leap_day) ELSE 0 END )::int AS won_renewal_multi_arr,
    SUM(CASE WHEN opportunity.isclosed AND NOT opportunity.iswon THEN 365*(opportunitylineitem.new_amount__c)/(opportunitylineitem.end_date__c::date - opportunitylineitem.start_date__c::date + 1 - crosses_leap_day)ELSE 0 END )::int AS lost_new_arr,
    SUM(CASE WHEN opportunity.isclosed AND NOT opportunity.iswon THEN 365*(opportunitylineitem.expansion_amount__c)/(opportunitylineitem.end_date__c::date - opportunitylineitem.start_date__c::date + 1 - crosses_leap_day)ELSE 0 END )::int AS lost_expansion_arr,
    SUM(CASE WHEN opportunity.isclosed AND NOT opportunity.iswon THEN 365*(opportunitylineitem.coterm_expansion_amount__c)/(opportunitylineitem.end_date__c::date - opportunitylineitem.start_date__c::date + 1 - crosses_leap_day)ELSE 0 END )::int AS lost_coterm_expansion_arr,
    SUM(CASE WHEN opportunity.isclosed AND NOT opportunity.iswon THEN 365*(opportunitylineitem.leftover_expansion_amount__c)/(opportunitylineitem.end_date__c::date - opportunitylineitem.start_date__c::date + 1 - crosses_leap_day)ELSE 0 END )::int AS lost_leftover_expansion_arr,
    SUM(CASE WHEN opportunity.isclosed AND NOT opportunity.iswon THEN 365*(opportunitylineitem.renewal_amount__c)/(opportunitylineitem.end_date__c::date - opportunitylineitem.start_date__c::date + 1 - crosses_leap_day)ELSE 0 END )::int AS lost_renewal_arr,
    SUM(CASE WHEN opportunity.isclosed AND NOT opportunity.iswon THEN 365*(opportunitylineitem.multi_amount__c)/(opportunitylineitem.end_date__c::date - opportunitylineitem.start_date__c::date + 1 - crosses_leap_day)ELSE 0 END )::int AS lost_multi_arr,
    SUM(CASE WHEN opportunity.isclosed AND NOT opportunity.iswon THEN 365*(opportunitylineitem.renewal_multi_amount__c)/(opportunitylineitem.end_date__c::date - opportunitylineitem.start_date__c::date + 1 - crosses_leap_day)ELSE 0 END )::int AS lost_renewal_multi_arr,
    SUM(CASE WHEN NOT opportunity.isclosed THEN 365*(opportunitylineitem.new_amount__c)/(opportunitylineitem.end_date__c::date - opportunitylineitem.start_date__c::date + 1 - crosses_leap_day) ELSE 0 END )::int AS open_new_arr,
    SUM(CASE WHEN NOT opportunity.isclosed THEN 365*(opportunitylineitem.expansion_amount__c)/(opportunitylineitem.end_date__c::date - opportunitylineitem.start_date__c::date + 1 - crosses_leap_day) ELSE 0 END )::int AS open_expansion_arr,
    SUM(CASE WHEN NOT opportunity.isclosed THEN 365*(opportunitylineitem.coterm_expansion_amount__c)/(opportunitylineitem.end_date__c::date - opportunitylineitem.start_date__c::date + 1 - crosses_leap_day) ELSE 0 END )::int AS open_coterm_expansion_arr,
    SUM(CASE WHEN NOT opportunity.isclosed THEN 365*(opportunitylineitem.leftover_expansion_amount__c)/(opportunitylineitem.end_date__c::date - opportunitylineitem.start_date__c::date + 1 - crosses_leap_day) ELSE 0 END )::int AS open_leftover_expansion_arr,
    SUM(CASE WHEN NOT opportunity.isclosed THEN 365*(opportunitylineitem.renewal_amount__c)/(opportunitylineitem.end_date__c::date - opportunitylineitem.start_date__c::date + 1 - crosses_leap_day) ELSE 0 END )::int AS open_renewal_arr,
    SUM(CASE WHEN NOT opportunity.isclosed THEN 365*(opportunitylineitem.multi_amount__c)/(opportunitylineitem.end_date__c::date - opportunitylineitem.start_date__c::date + 1 - crosses_leap_day) ELSE 0 END )::int AS open_multi_arr,
    SUM(CASE WHEN NOT opportunity.isclosed THEN 365*(opportunitylineitem.renewal_multi_amount__c)/(opportunitylineitem.end_date__c::date - opportunitylineitem.start_date__c::date + 1 - crosses_leap_day) ELSE 0 END )::int AS open_renewal_multi_arr
  FROM {{ ref( 'opportunitylineitem') }}  AS opportunitylineitem
  LEFT JOIN opportunitylineitems_impacted ON opportunitylineitems_impacted.opportunitylineitem_sfid = opportunitylineitem.sfid
  LEFT JOIN {{ ref( 'opportunity') }}  AS opportunity ON opportunity.sfid = opportunitylineitem.opportunityid
  LEFT JOIN {{ ref('dates') }}  AS util_dates ON util_dates.date::date >= opportunitylineitem.start_date__c::date AND util_dates.date::date <= opportunitylineitem.end_date__c::date
  WHERE opportunitylineitem.end_date__c::date-opportunitylineitem.start_date__c::date <> 0 AND opportunitylineitem.end_date__c::date - opportunitylineitem.start_date__c::date + 1 - crosses_leap_day <> 0 AND opportunitylineitem.product_type__c = 'Recurring'
  GROUP BY 1, 2, 3
)
SELECT * FROM opportunitylineitem_daily_arr