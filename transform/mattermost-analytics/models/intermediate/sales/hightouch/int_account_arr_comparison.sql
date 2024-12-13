{{
  config({
    "materialized": 'table'
  })
}}

WITH dates as (
  {{
    dbt_utils.date_spine(
      start_date="to_date('02/01/2010', 'mm/dd/yyyy')",
      datepart="day",
      end_date="dateadd(year, 5, current_date)"
     )
  }}
), leap_years AS (
    SELECT
        date_day as date
    FROM dates
    WHERE date_day::varchar LIKE '%-02-29'
    GROUP BY 1

), opportunitylineitems_impacted AS (
    SELECT
        o.opportunity_id,
        oli.opportunity_line_item_id,
        MAX(CASE WHEN leap_years.date BETWEEN start_date__c::date AND end_date__c::date THEN 1 ELSE 0 END) AS crosses_leap_day
    FROM
        {{ ref('stg_salesforce__opportunity') }} o
        LEFT JOIN {{ ref('stg_salesforce__opportunity_line_item') }} oli ON o.opportunity_id = oli.opportunity_id
        LEFT JOIN leap_years ON 1 = 1
    GROUP BY o.opportunity_id, oli.opportunity_line_item_id
)
SELECT
    a.account_id AS account_id,
    oli.end_date__c::date - oli.start_date__c::date + 1 - crosses_leap_day AS days,
    datediff(day, oli.start_date__c::date, oli.end_date__c::date)  AS days_diff,
    crosses_leap_day
FROM
    {{ ref('stg_salesforce__opportunity_line_item') }} oli
    LEFT JOIN opportunitylineitems_impacted ON opportunitylineitems_impacted.opportunity_line_item_id = oli.opportunity_line_item_id
    LEFT JOIN {{ ref('stg_salesforce__opportunity') }} o ON o.opportunity_id = oli.opportunity_id
    LEFT JOIN {{ ref('stg_salesforce__account') }} a ON a.account_id = o.account_id
WHERE
    o.is_won
    AND oli.end_date__c::date - oli.start_date__c::date <> 0
    AND current_date >= oli.start_date__c::date
    AND current_date <= oli.end_date__c::date
