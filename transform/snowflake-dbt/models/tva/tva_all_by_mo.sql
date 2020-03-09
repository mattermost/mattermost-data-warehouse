{{config({
    "materialized": 'table',
    "schema": "tva"
  })
}}

WITH tva_all_by_mo AS (
    SELECT 'ARR by Month' as target_name, 'Finance' as target_category, *
    FROM {{ ref('tva_arr_by_mo') }}

    UNION ALL

    SELECT 'ARR Churn by Month' as target_name, 'Finance' as target_category, *
    FROM {{ ref('tva_arr_churn_by_mo') }}

    UNION ALL

    SELECT 'ARR New by Month' as target_name, 'Finance' as target_category, *
    FROM {{ ref('tva_arr_new_by_mo') }}

    UNION ALL

    SELECT 'ARR Expansion by Month' as target_name, 'Finance' as target_category, *
    FROM {{ ref('tva_arr_exp_by_mo') }}

    UNION ALL

    SELECT 'TEDAS (7 Day Active) by Month', 'Product', *
    FROM {{ ref('tva_tedas_7day_by_mo') }}
)

SELECT * FROM tva_all_by_mo