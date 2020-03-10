{{config({
    "materialized": 'table',
    "schema": "tva"
  })
}}

WITH tva_bookings_ren_by_qtr AS (
    SELECT
        'bookings_ren_by_qtr' AS target_slug,
        util.fiscal_year(tva_bookings_ren_by_mo.month)|| '-' || util.fiscal_quarter(tva_bookings_ren_by_mo.month) AS qtr,
        max(period_last_day) AS period_last_day,
        sum(tva_bookings_ren_by_mo.target) AS target,
        sum(tva_bookings_ren_by_mo.actual) as actual,
        round(sum(tva_bookings_ren_by_mo.actual)/sum(tva_bookings_ren_by_mo.target),2) AS tva
    FROM {{ ref('tva_bookings_ren_by_mo') }}
    GROUP BY 1,2
)

SELECT * FROM tva_bookings_ren_by_qtr