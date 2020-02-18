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
), oppt_w_arr AS (
  SELECT
    opportunity.sfid AS opportunity_sfid,
  	SUM(365*(opportunitylineitem.totalprice)/(opportunitylineitem.end_date__c::date - opportunitylineitem.start_date__c::date + 1 - crosses_leap_day))::int AS total_arr
  FROM orgm.opportunitylineitem
  LEFT JOIN opportunitylineitems_impacted ON opportunitylineitems_impacted.opportunitylineitem_sfid = opportunitylineitem.sfid
  LEFT JOIN orgm.opportunity ON opportunity.sfid = opportunitylineitem.opportunityid
  WHERE opportunity.iswon
    AND opportunitylineitem.end_date__c::date-opportunitylineitem.start_date__c::date <> 0
  GROUP BY 1
), all_oppt_arr AS (
  SELECT
    opportunity.sfid AS opportunity_sfid,
    COALESCE(total_arr,0) AS total_arr
  FROM orgm.opportunity
  LEFT JOIN oppt_w_arr ON opportunity.sfid = oppt_w_arr.opportunity_sfid
)
UPDATE orgm.opportunity
    SET arr_contributed__c = all_oppt_arr.total_arr
FROM all_oppt_arr
WHERE all_oppt_arr.opportunity_sfid = opportunity.sfid
    AND (all_oppt_arr.total_arr <> opportunity.arr_contributed__c OR opportunity.arr_contributed__c ISNULL);

COMMIT;