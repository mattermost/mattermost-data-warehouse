{{config({
    "materialized": 'table',
    "schema": "tva"
  })
}}

WITH tva_all_by_fy AS (

  SELECT target_slug,fy,period_first_day,period_last_day,target,actual,tva
  FROM {{ ref('tva_arr_by_fy') }}

  UNION ALL

  SELECT target_slug,fy,period_first_day,period_last_day,target,actual,tva
  FROM {{ ref('tva_bookings_ren_by_fy') }}

  UNION ALL

  SELECT target_slug,month,period_first_day,period_last_day,target,actual,tva
  FROM {{ ref('tva_bookings_new_by_fy') }}

  UNION ALL

  SELECT target_slug,month,period_first_day,period_last_day,target,actual,tva
  FROM {{ ref('tva_bookings_exp_by_fy') }}

  UNION ALL

  SELECT target_slug,month,period_first_day,period_last_day,target,actual,tva
  FROM {{ ref('tva_bookings_new_and_exp_by_fy') }}

  UNION ALL

  SELECT target_slug,fy,period_first_day,period_last_day,target,actual,tva
  FROM {{ ref('tva_downloads_by_fy') }}

)

SELECT * FROM tva_all_by_fy