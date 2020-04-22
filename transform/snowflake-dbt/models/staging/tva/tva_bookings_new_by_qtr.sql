{{config({
    "materialized": 'table',
    "schema": "staging"
  })
}}

WITH tva_bookings_new_by_qtr AS (
    SELECT
        'bookings_new_by_qtr' AS target_slug,
        util.fiscal_year(tva_bookings_new_by_mo.month)|| '-' || util.fiscal_quarter(tva_bookings_new_by_mo.month) AS qtr,
        min(period_first_day) AS period_first_day,
        max(period_last_day) AS period_last_day,
        sum(tva_bookings_new_by_mo.target) AS target,
        sum(tva_bookings_new_by_mo.actual) AS actual,
        round(sum(tva_bookings_new_by_mo.actual)/sum(tva_bookings_new_by_mo.target),2) AS tva,
        sum(tva_bookings_new_by_mo.actual_oppt_less_than_5k) AS actual_oppt_less_than_5k
    FROM {{ ref('tva_bookings_new_by_mo') }}
    GROUP BY 1,2
)

SELECT * FROM tva_bookings_new_by_qtr