{{config({
    "materialized": 'table',
    "schema": "tva"
  })
}}

WITH github_contributors AS (
    SELECT 
        author, 
        MIN(merged_at) AS min_contribution, 
        MAX(merged_at) AS max_contribution,
        COUNT(*) AS total_contributions
    FROM {{ source('targets', 'curr_fy_arr_by_mo') }}
    GROUP BY 1
)

SELECT * FROM curr_fy_arr_by_mo