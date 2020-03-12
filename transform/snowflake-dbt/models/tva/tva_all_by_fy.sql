{{config({
    "materialized": 'table',
    "schema": "tva"
  })
}}

WITH tva_all_by_fy AS (

  SELECT *
  FROM {{ ref('tva_arr_by_fy') }}

  UNION ALL

  SELECT *
  FROM {{ ref('tva_bookings_ren_by_fy') }}

  UNION ALL

  SELECT *
  FROM {{ ref('tva_downloads_by_fy') }}

)

SELECT * FROM tva_all_by_fy