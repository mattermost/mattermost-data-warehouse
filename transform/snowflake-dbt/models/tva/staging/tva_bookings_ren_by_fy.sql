{{config({
    "materialized": 'table',
    "schema": "tva"
  })
}}

WITH tva_bookings_ren_by_fy AS (
    SELECT
        'bookings_ren_by_fy' AS target_slug,
        util.fiscal_year(tva_bookings_ren_by_mo.month) AS fy,
        sum(bookings_ren_by_qtr.target) AS target,
        sum(actual_bookings_ren_by_qtr.actual) as actual,
        round(sum(actual_bookings_ren_by_qtr.actual/sum(bookings_ren_by_qtr.target)),2) AS tva
    FROM {{ ref('tva_bookings_ren_by_mo') }}
    GROUP BY 1,2
)

SELECT * FROM tva_bookings_ren_by_fy