{{config({
    "materialized": 'table',
    "schema": "tva"
  })
}}

WITH bookings_new AS (
    SELECT
      opportunity.closedate,
      opportunity.sfid AS opportunity_sfid,
      opportunitylineitem.sfid AS opportunitylineitem_sfid,
      opportunity.type AS opportunity_type,
      opportunitylineitem.product_line_type__c AS product_line_type,
      CASE WHEN end_date__c::date - start_date__c::date + 1 > 365 THEN opportunitylineitem.arr_contributed__c ELSE opportunitylineitem.totalprice END AS bookings
    FROM {{ source('orgm', 'opportunity') }} AS opportunity
    LEFT JOIN {{ source('orgm', 'opportunitylineitem') }} AS opportunitylineitem ON opportunity.sfid = opportunitylineitem.opportunityid
    WHERE iswon AND opportunitylineitem.product_line_type__c = 'New'
), actual_bookings_new_by_mo AS (
    SELECT 
        date_trunc('month', closedate)::date AS month,
        sum(bookings) AS total_bookings
    FROM bookings_new
    GROUP BY 1
), tva_bookings_new_by_mo AS (
    SELECT
        'bookings_new_by_mo' as target_slug,
        bookings_new_by_mo.month,
        bookings_new_by_mo.target,
        actual_bookings_new_by_mo.total_bookings as actual,
        round((actual_bookings_new_by_mo.total_bookings/bookings_new_by_mo.target),2) as tva
    FROM {{ source('targets', 'bookings_new_by_mo') }}
    LEFT JOIN actual_bookings_new_by_mo ON bookings_new_by_mo.month = actual_bookings_new_by_mo.month
)

SELECT * FROM tva_bookings_new_by_mo