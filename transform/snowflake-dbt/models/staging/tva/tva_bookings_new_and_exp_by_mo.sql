{{config({
    "materialized": 'table',
    "schema": "staging"
  })
}}

WITH tva_bookings_new_and_exp_by_mo AS (
    SELECT
        'bookings_new_and_exp_by_mo' AS target_slug,
        bookings_new_and_exp_by_mo.month,
        bookings_new_and_exp_by_mo.month AS period_first_day,
        bookings_new_and_exp_by_mo.month + interval '1 month' - interval '1 day' AS period_last_day,
        bookings_new_and_exp_by_mo.target,
        COALESCE(tva_bookings_new_by_mo.actual,0) + COALESCE(tva_bookings_exp_by_mo.actual,0) AS actual,
        round(((COALESCE(tva_bookings_new_by_mo.actual,0)+COALESCE(tva_bookings_exp_by_mo.actual,0))/bookings_new_and_exp_by_mo.target),3) AS tva
    FROM {{ source('targets', 'bookings_new_and_exp_by_mo') }}
    LEFT JOIN {{ ref('tva_bookings_exp_by_mo') }} ON bookings_new_and_exp_by_mo.month = tva_bookings_exp_by_mo.month
    LEFT JOIN {{ ref('tva_bookings_new_by_mo') }} ON tva_bookings_exp_by_mo.month = tva_bookings_new_by_mo.month
)

SELECT * FROM tva_bookings_new_and_exp_by_mo