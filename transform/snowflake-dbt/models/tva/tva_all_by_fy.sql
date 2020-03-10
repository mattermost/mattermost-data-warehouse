{{config({
    "materialized": 'table',
    "schema": "tva"
  })
}}

WITH tva_all_by_fy_raw AS (

  SELECT *
  FROM {{ ref('tva_arr_by_fy') }}

  UNION ALL

  SELECT *
  FROM {{ ref('tva_bookings_ren_by_fy') }}

), tva_all_by_fy AS (

  SELECT target_fact.name, target_fact.category, tva_all_by_fy_raw.*
  FROM {{ source('targets', 'target_fact') }}
  JOIN tva_all_by_fy_raw ON tva_all_by_fy_raw.target_slug = target_fact.slug
  
)

SELECT * FROM tva_all_by_fy