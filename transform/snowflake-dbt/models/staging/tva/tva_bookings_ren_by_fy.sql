{{config({
    "materialized": 'table',
    "schema": "staging"
  })
}}

WITH tva_bookings_ren_by_fy AS (
    SELECT
        'bookings_ren_by_fy' AS target_slug,
        util.fiscal_year(tva_bookings_ren_by_qtr.period_first_day) AS fy,
        min(period_first_day) AS period_first_day,
        max(period_last_day) AS period_last_day,
        sum(tva_bookings_ren_by_qtr.target) AS target,
        sum(tva_bookings_ren_by_qtr.actual) AS actual,
        round(sum(tva_bookings_ren_by_qtr.actual)/sum(tva_bookings_ren_by_qtr.target),3) AS tva
    FROM {{ ref('tva_bookings_ren_by_qtr') }}
    GROUP BY 1,2
)

SELECT * FROM tva_bookings_ren_by_fy