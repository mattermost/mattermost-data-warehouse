{{config({
    "materialized": 'table',
    "schema": "staging"
  })
}}

WITH actual_bookings_ren_by_qtr AS (
    SELECT
      util.fiscal_year(bookings_ren.closedate)|| '-' || util.fiscal_quarter(bookings_ren.closedate) AS qtr,
      ROUND(SUM(renewal_amount__c)) AS actual
    FROM {{ source('orgm', 'opportunity') }} AS opportunity
    LEFT JOIN {{ source('orgm', 'opportunitylineitem') }} AS opportunitylineitem ON opportunity.sfid = opportunitylineitem.opportunityid
    WHERE iswon AND opportunitylineitem.product_line_type__c = 'Ren'
), tva_bookings_ren_by_qtr AS (
    SELECT
        'bookings_ren_by_qtr' AS target_slug,
        bookings_ren_by_qtr.qtr,
        util.fiscal_quarter_start(bookings_ren_by_qtr.qtr) AS  period_first_day,
        util.fiscal_quarter_end(bookings_ren_by_qtr.qtr) AS  period_last_day,
        bookings_ren_by_qtr.target,
        actual_bookings_ren_by_qtr.total_bookings AS actual,
        round((actual_bookings_ren_by_qtr.total_bookings/bookings_ren_by_qtr.target),2) AS tva
    FROM {{ source('targets', 'bookings_ren_by_qtr') }}
    LEFT JOIN actual_bookings_ren_by_qtr ON bookings_ren_by_qtr.qtr = actual_bookings_ren_by_qtr.qtr
)

SELECT * FROM tva_bookings_ren_by_qtr