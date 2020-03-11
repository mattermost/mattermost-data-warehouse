{{config({
    "materialized": 'table',
    "schema": "tva"
  })
}}

WITH tva_all_by_mo AS (
  SELECT *
  FROM {{ ref('tva_arr_by_mo') }}

  UNION ALL

  SELECT *
  FROM {{ ref('tva_arr_churn_by_mo') }}

  UNION ALL

  SELECT *
  FROM {{ ref('tva_arr_new_by_mo') }}

  UNION ALL

  SELECT *
  FROM {{ ref('tva_arr_exp_by_mo') }}

  UNION ALL

  SELECT *
  FROM {{ ref('tva_bookings_by_mo') }}

  UNION ALL

  SELECT *
  FROM {{ ref('tva_bookings_ren_by_mo') }}

  UNION ALL

  SELECT *
  FROM {{ ref('tva_bookings_new_by_mo') }}

  UNION ALL

  SELECT *
  FROM {{ ref('tva_bookings_exp_by_mo') }}

  UNION ALL

  SELECT *
  FROM {{ ref('tva_tedas_7day_by_mo') }}

  UNION ALL

  SELECT *
  FROM {{ ref('tva_downloads_by_mo') }}
)

SELECT * FROM tva_all_by_mo