{{config({
    "materialized": 'table',
    "schema": "tva"
  })
}}

WITH tva_all_by_mo AS (
  SELECT target_slug,month,period_first_day,period_last_day,target,actual,tva
  FROM {{ ref('tva_arr_by_mo') }}

  UNION ALL

  SELECT target_slug,month,period_first_day,period_last_day,target,actual,tva
  FROM {{ ref('tva_arr_churn_by_mo') }}

  UNION ALL

  SELECT target_slug,month,period_first_day,period_last_day,target,actual,tva
  FROM {{ ref('tva_arr_new_by_mo') }}

  UNION ALL

  SELECT target_slug,month,period_first_day,period_last_day,target,actual,tva
  FROM {{ ref('tva_arr_exp_by_mo') }}

  UNION ALL

  SELECT target_slug,month,period_first_day,period_last_day,target,actual,tva
  FROM {{ ref('tva_bookings_by_mo') }}

  UNION ALL

  SELECT target_slug,month,period_first_day,period_last_day,target,actual,tva
  FROM {{ ref('tva_bookings_new_by_mo') }}

  UNION ALL

  SELECT target_slug,month,period_first_day,period_last_day,target,actual,tva
  FROM {{ ref('tva_bookings_exp_by_mo') }}

  UNION ALL

  SELECT target_slug,month,period_first_day,period_last_day,target,actual,tva
  FROM {{ ref('tva_bookings_new_and_exp_by_mo') }}

  UNION ALL

  SELECT target_slug,month,period_first_day,period_last_day,target,actual,tva
  FROM {{ ref('tva_tedas_7day_by_mo') }}

  UNION ALL

  SELECT target_slug,month,period_first_day,period_last_day,target,actual,tva
  FROM {{ ref('tva_downloads_by_mo') }}

  UNION ALL

  SELECT target_slug,month,period_first_day,period_last_day,target,actual,tva
  FROM {{ ref('tva_bookings_new_and_exp_by_rep_by_mo') }}

)

SELECT * FROM tva_all_by_mo