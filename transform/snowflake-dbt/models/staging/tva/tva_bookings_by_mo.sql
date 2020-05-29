{{config({
    "materialized": 'table',
    "schema": "staging"
  })
}}

WITH actual_bookings_by_mo AS (
    SELECT
      date_trunc('month', closedate)::date AS month,
      ROUND(SUM(new_amount__c + expansion_amount__c + coterm_expansion_amount__c + leftover_expansion_amount__c + multi_amount__c),2) AS actual
    FROM {{ source('orgm', 'opportunity') }} AS opportunity
    LEFT JOIN {{ source('orgm', 'opportunitylineitem') }} AS opportunitylineitem ON opportunity.sfid = opportunitylineitem.opportunityid
    WHERE iswon
), tva_bookings_by_mo AS (
    SELECT
        'bookings_by_mo' AS target_slug,
        bookings_by_mo.month,
        bookings_by_mo.month AS period_first_day,
        bookings_by_mo.month + interval '1 month' - interval '1 day' AS period_last_day,
        bookings_by_mo.target,
        actual_bookings_by_mo.actual AS actual,
        round((actual_bookings_by_mo.actual/bookings_by_mo.target),2) AS tva
    FROM {{ source('targets', 'bookings_by_mo') }}
    LEFT JOIN actual_bookings_by_mo ON bookings_by_mo.month = actual_bookings_by_mo.month
)

SELECT * FROM tva_bookings_by_mo