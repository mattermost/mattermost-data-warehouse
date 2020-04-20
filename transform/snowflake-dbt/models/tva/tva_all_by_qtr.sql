{{config({
    "materialized": 'table',
    "schema": "tva"
  })
}}

WITH tva_all_by_qtr AS (

  SELECT target_slug,qtr,period_first_day,period_last_day,target,actual,tva
  FROM {{ ref('tva_arr_by_qtr') }}

  UNION ALL

  SELECT target_slug,qtr,period_first_day,period_last_day,target,actual,tva
  FROM {{ ref('tva_bookings_ren_by_qtr') }}

  UNION ALL

  SELECT target_slug,qtr,period_first_day,period_last_day,target,actual,tva
  FROM {{ ref('tva_bookings_new_by_qtr') }}

  UNION ALL

  SELECT target_slug,qtr,period_first_day,period_last_day,target,actual,tva
  FROM {{ ref('tva_bookings_exp_by_qtr') }}

  UNION ALL

  SELECT target_slug,qtr,period_first_day,period_last_day,target,actual,tva
  FROM {{ ref('tva_bookings_new_and_exp_by_qtr') }}

  UNION ALL

  SELECT target_slug,qtr,period_first_day,period_last_day,target,actual,tva
  FROM {{ ref('tva_downloads_by_qtr') }}

)

SELECT * FROM tva_all_by_qtr