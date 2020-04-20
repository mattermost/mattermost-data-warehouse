{{config({
    "materialized": 'table',
    "schema": "staging"
  })
}}

WITH actual_bookings_new_by_mo AS (
    SELECT
        DATE_TRUNC('month', opportunity.closedate) AS month,
        SUM(CASE WHEN opportunitylineitem.product_line_type__c = 'New' THEN opportunitylineitem.totalprice ELSE NULL END) AS total_bookings
    FROM {{ source('orgm','account') }}
    LEFT JOIN {{ source('orgm','opportunity') }} ON account.sfid = opportunity.accountid
    LEFT JOIN {{ ref('opportunity_ext') }} ON opportunity.sfid = opportunity_ext.opportunity_sfid
    LEFT JOIN {{ source('orgm','opportunitylineitem') }} ON opportunity.sfid = opportunitylineitem.opportunityid
    WHERE util.fiscal_year(closedate) =  util.get_sys_var('curr_fy') AND opportunity.iswon AND (opportunity_ext.sum_new_amount + opportunity_ext.sum_expansion_w_proration_amount  >= 5000)
    GROUP BY 1
), tva_bookings_new_by_mo AS (
    SELECT
        'bookings_new_by_mo' AS target_slug,
        bookings_new_by_mo.month,
        bookings_new_by_mo.month AS period_first_day,
        bookings_new_by_mo.month + interval '1 month' - interval '1 day' AS period_last_day,
        bookings_new_by_mo.target,
        actual_bookings_new_by_mo.total_bookings AS actual,
        round((actual_bookings_new_by_mo.total_bookings/bookings_new_by_mo.target),2) AS tva
    FROM {{ source('targets', 'bookings_new_by_mo') }}
    LEFT JOIN actual_bookings_new_by_mo ON bookings_new_by_mo.month = actual_bookings_new_by_mo.month
)

SELECT * FROM tva_bookings_new_by_mo