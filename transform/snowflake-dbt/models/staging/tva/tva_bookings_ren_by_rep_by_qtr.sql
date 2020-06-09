{{config({
    "materialized": 'table',
    "schema": "staging"
  })
}}

WITH actual_bookings_ren_by_rep_by_qtr AS (
    SELECT
	    user.employeenumber,
	    util.fiscal_year(opportunity.closedate)|| '-' || util.fiscal_quarter(opportunity.closedate) AS qtr,
	    ROUND(SUM(renewal_amount__c),2) AS actual
    FROM {{ source('orgm','account') }}
        LEFT JOIN {{ source('orgm','opportunity') }} ON account.sfid = opportunity.accountid
        LEFT JOIN {{ source('orgm','opportunitylineitem') }} ON opportunity.sfid = opportunitylineitem.opportunityid
        LEFT JOIN {{ source('orgm','user') }} ON opportunity.csm_owner__c = user.sfid
    WHERE util.fiscal_year(closedate) = util.get_sys_var('curr_fy') AND opportunity.iswon
    GROUP BY 1,2
), tva_bookings_ren_by_rep_by_qtr AS (
    SELECT
        'bookings_ren_by_rep_by_qtr'||'_'||bookings_ren_by_rep_by_qtr.rep_emp_num AS target_slug,
        bookings_ren_by_rep_by_qtr.qtr,
        util.fiscal_quarter_start(bookings_ren_by_rep_by_qtr.qtr) AS  period_first_day,
        util.fiscal_quarter_end(bookings_ren_by_rep_by_qtr.qtr) AS  period_last_day,
        bookings_ren_by_rep_by_qtr.target,
        COALESCE(actual_bookings_ren_by_rep_by_qtr.actual,0) AS actual,
        ROUND(COALESCE(actual_bookings_ren_by_rep_by_qtr.actual,0)/bookings_ren_by_rep_by_qtr.target,3) AS tva
    FROM {{ source('targets', 'bookings_ren_by_rep_by_qtr') }}
    LEFT JOIN actual_bookings_ren_by_rep_by_qtr 
        ON bookings_ren_by_rep_by_qtr.qtr = actual_bookings_ren_by_rep_by_qtr.qtr
            AND bookings_ren_by_rep_by_qtr.rep_emp_num = actual_bookings_ren_by_rep_by_qtr.employeenumber
)

SELECT * FROM tva_bookings_ren_by_rep_by_qtr