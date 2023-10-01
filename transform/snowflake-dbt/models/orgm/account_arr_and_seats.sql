{{config({
    "materialized": 'table',
    "schema": "orgm",
  })
}}

WITH leap_years AS (
    SELECT 
        dates.date AS date
    FROM {{ ref('dates') }}
    WHERE dates.date::varchar LIKE '%-02-29'
    GROUP BY 1
), opportunitylineitems_impacted AS (
    SELECT
        opportunity.sfid AS opportunity_sfid,
        opportunitylineitem.sfid AS opportunitylineitem_sfid,
        MAX(CASE WHEN leap_years.date BETWEEN start_date__c::date AND end_date__c::date THEN 1 ELSE 0 END) AS crosses_leap_day
    FROM {{ ref('opportunity') }}
    LEFT JOIN {{ ref('opportunitylineitem') }} ON opportunity.sfid = opportunitylineitem.opportunityid
    LEFT JOIN leap_years ON 1 = 1
    GROUP BY opportunity_sfid, opportunitylineitem_sfid
), account_w_arr AS (
    SELECT
        account.sfid AS account_sfid,
        SUM(365*(opportunitylineitem.totalprice)/(opportunitylineitem.end_date__c::date - opportunitylineitem.start_date__c::date + 1 - crosses_leap_day)) AS total_arr
    FROM {{ ref('opportunitylineitem') }}
    LEFT JOIN opportunitylineitems_impacted ON opportunitylineitems_impacted.opportunitylineitem_sfid = opportunitylineitem.sfid
    LEFT JOIN {{ ref('opportunity') }} ON opportunity.sfid = opportunitylineitem.opportunityid
    LEFT JOIN {{ ref('account') }} ON account.sfid = opportunity.accountid
    WHERE opportunity.iswon
        AND opportunitylineitem.end_date__c::date-opportunitylineitem.start_date__c::date <> 0
        AND current_date >= opportunitylineitem.start_date__c::date
        AND current_date <= opportunitylineitem.end_date__c::date
    GROUP BY 1
), seats_licensed AS (
    SELECT account.sfid AS account_sfid,
        SUM(CASE WHEN product2.family = 'License' THEN opportunitylineitem.quantity ELSE 0 END) AS seats
    FROM {{ ref('account') }}
    LEFT JOIN {{ ref('opportunity') }} ON account.sfid = opportunity.accountid AND opportunity.status_wlo__c = 'Won'
    LEFT JOIN {{ ref('opportunitylineitem') }} ON opportunity.sfid = opportunitylineitem.opportunityid
    LEFT JOIN {{ ref('product2') }} ON opportunitylineitem.product2id = product2.sfid
    WHERE current_date >= opportunitylineitem.start_date__c AND current_date <= opportunitylineitem.end_date__c
    GROUP BY 1
    HAVING SUM(CASE WHEN product2.family = 'License' THEN opportunitylineitem.quantity ELSE 0 END) > 0
), account_arr_and_seats AS (
    SELECT
        account.sfid AS account_sfid,
        GREATEST(COALESCE(total_arr, 0),0) AS total_arr,
        GREATEST(COALESCE(seats, 0),0) AS seats
    FROM {{ ref('account') }}
    LEFT JOIN account_w_arr ON account.sfid = account_w_arr.account_sfid
    LEFT JOIN seats_licensed ON account.sfid = seats_licensed.account_sfid
)

SELECT * FROM account_arr_and_seats
