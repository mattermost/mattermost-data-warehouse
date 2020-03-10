{{config({
    "materialized": 'table',
    "schema": "tva"
  })
}}

WITH bookings_ren AS (
    SELECT
      opportunity.closedate,
      opportunity.sfid AS opportunity_sfid,
      opportunitylineitem.sfid AS opportunitylineitem_sfid,
      opportunity.type AS opportunity_type,
      opportunitylineitem.product_line_type__c AS product_line_type,
      CASE WHEN end_date__c::date - start_date__c::date + 1 > 365 THEN opportunitylineitem.arr_contributed__c ELSE opportunitylineitem.totalprice END AS bookings
    FROM {{ source('orgm', 'opportunity') }} AS opportunity
    LEFT JOIN {{ source('orgm', 'opportunitylineitem') }} AS opportunitylineitem ON opportunity.sfid = opportunitylineitem.opportunityid
    WHERE iswon AND opportunitylineitem.product_line_type__c = 'Ren'
), actual_bookings_ren_by_qtr AS (
    SELECT 
        util.fiscal_year(closedate)|| '-' || util.fiscal_quarter(closedate) AS qtr,
        sum(bookings) AS total_bookings
    FROM bookings_ren
    GROUP BY 1
), bookings_ren_by_qtr AS (
    SELECT 
        util.fiscal_year(month)|| '-' || util.fiscal_quarter(month) AS qtr,
        SUM(target) as target
    FROM {{ source('targets', 'bookings_ren_by_mo') }}
    GROUP BY 1
), tva_bookings_ren_by_qtr AS (
    SELECT
        'bookings_ren_by_qtr' as target_slug,
        bookings_ren_by_qtr.qtr,
        bookings_ren_by_qtr.target,
        actual_bookings_ren_by_qtr.total_bookings as actual,
        round((actual_bookings_ren_by_qtr.total_bookings/bookings_ren_by_qtr.target),2) as tva
    FROM bookings_ren_by_qtr
    LEFT JOIN actual_bookings_ren_by_qtr ON bookings_ren_by_qtr.qtr = actual_bookings_ren_by_qtr.qtr
)

SELECT * FROM tva_bookings_ren_by_qtr