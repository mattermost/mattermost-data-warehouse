{{config({
    "materialized": 'table',
    "schema": "tva"
  })
}}

WITH tva_all_by_mo AS (
    SELECT 'ARR by Month' as target_name, 'Finance' as target_category, *
    FROM {{ ref('tva_curr_fy_arr_by_mo') }}

    UNION ALL

    SELECT 'TEDAS (7 Day Active) by Month', 'Product', *
    FROM {{ ref('tva_curr_fy_tedas_7day_by_mo') }}
)

SELECT * FROM tva_all_by_mo