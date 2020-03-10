{{config({
    "materialized": 'table',
    "schema": "tva"
  })
}}

WITH tva_all_by_qtr_raw AS (
  SELECT *
  FROM {{ ref('tva_arr_by_qtr') }}
), tva_all_by_qtr AS (
  SELECT target_fact.name, target_fact.category, tva_all_by_qtr_raw.*
  FROM {{ source('targets', 'target_fact') }}
  JOIN tva_all_by_qtr_raw ON tva_all_by_wtr_raw.target_slug = target_fact.slug
)

SELECT * FROM tva_all_by_qtr