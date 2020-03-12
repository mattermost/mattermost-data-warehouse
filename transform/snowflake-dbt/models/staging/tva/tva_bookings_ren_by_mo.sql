{{config({
    "materialized": 'table',
    "schema": "staging"
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
), actual_bookings_ren_by_mo AS (
    SELECT 
        date_trunc('month', closedate)::date AS month,
        sum(bookings) AS total_bookings
    FROM bookings_ren
    GROUP BY 1
), tva_bookings_ren_by_mo AS (
    SELECT
        'bookings_ren_by_mo' AS target_slug,
        bookings_ren_by_mo.month,
        bookings_ren_by_mo.month AS period_first_day,
        bookings_ren_by_mo.month + interval '1 month' - interval '1 day' AS period_last_day,
        bookings_ren_by_mo.target,
        actual_bookings_ren_by_mo.total_bookings AS actual,
        round((actual_bookings_ren_by_mo.total_bookings/bookings_ren_by_mo.target),2) AS tva
    FROM {{ source('targets', 'bookings_ren_by_mo') }}
    LEFT JOIN actual_bookings_ren_by_mo ON bookings_ren_by_mo.month = actual_bookings_ren_by_mo.month
)

SELECT * FROM tva_bookings_ren_by_mo