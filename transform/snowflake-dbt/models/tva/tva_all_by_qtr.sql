{{config({
    "materialized": 'table',
    "schema": "tva"
  })
}}

WITH tva_all_by_qtr AS (

  SELECT *
  FROM {{ ref('tva_arr_by_qtr') }}

  UNION ALL

  SELECT *
  FROM {{ ref('tva_bookings_ren_by_qtr') }}

  UNION ALL

  SELECT *
  FROM {{ ref('tva_downloads_by_qtr') }}

)

SELECT * FROM tva_all_by_qtr