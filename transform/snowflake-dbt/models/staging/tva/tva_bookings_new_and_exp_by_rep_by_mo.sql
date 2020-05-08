{{config({
    "materialized": 'table',
    "schema": "staging"
  })
}}

WITH actual_bookings_new_and_exp_by_rep_by_mo AS (
    SELECT
	    rep.sfid AS rep_emp_num,
	    DATE_TRUNC('month', opportunity.closedate) AS month,
	    ROUND(SUM(CASE WHEN opportunitylineitem.product_line_type__c IN ('New','Expansion') THEN opportunitylineitem.totalprice ELSE NULL END),2) AS actual
    FROM {{ source('orgm','opportunity') }}
        LEFT JOIN {{ source('orgm','opportunitylineitem') }} ON opportunity.sfid = opportunitylineitem.opportunityid
        LEFT JOIN {{ source('orgm','user') }} AS rep ON opportunity.ownerid = rep.sfid
    WHERE util.fiscal_year(closedate) = util.get_sys_var('curr_fy') AND opportunity.iswon
    GROUP BY 1,2
), tva_bookings_new_and_exp_by_rep_by_mo AS (
    SELECT
        'bookings_new_and_exp_by_rep_by_mo'||'_'||bookings_new_and_exp_by_rep_by_mo.rep_emp_num AS target_slug,
        bookings_new_and_exp_by_rep_by_mo.month,
        bookings_new_and_exp_by_rep_by_mo.month AS period_first_day,
        bookings_new_and_exp_by_rep_by_mo.month + interval '1 month' - interval '1 day' AS period_last_day,
        bookings_new_and_exp_by_rep_by_mo.target,
        COALESCE(actual_bookings_new_and_exp_by_rep_by_mo.actual,0) AS actual,
        ROUND(COALESCE(actual_bookings_new_and_exp_by_rep_by_mo.actual,0)/bookings_new_and_exp_by_rep_by_mo.target,2) AS tva
    FROM {{ source('targets', 'bookings_new_and_exp_by_rep_by_mo') }}
    LEFT JOIN actual_bookings_new_and_exp_by_rep_by_mo 
        ON bookings_new_and_exp_by_rep_by_mo.rep_emp_num = actual_bookings_new_and_exp_by_rep_by_mo.rep_emp_num
            AND bookings_new_and_exp_by_rep_by_mo.month = actual_bookings_new_and_exp_by_rep_by_mo.month
)

SELECT * FROM tva_bookings_new_and_exp_by_rep_by_mo