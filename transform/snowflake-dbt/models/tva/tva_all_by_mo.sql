{{config({
    "materialized": 'table',
    "schema": "tva"
  })
}}

WITH tva_all_by_mo_raw AS (
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
  FROM {{ ref('tva_tedas_7day_by_mo') }}
), tva_all_by_mo AS (
  SELECT target_fact.name, target_fact.category, tva_all_by_mo_raw.*
  FROM {{ source('targets', 'target_fact') }}
  JOIN tva_all_by_mo_raw ON tva_all_by_mo_raw.target_slug = target_fact.slug
)

SELECT * FROM tva_all_by_mo