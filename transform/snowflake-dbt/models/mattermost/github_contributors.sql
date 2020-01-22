{{config({
    "materialized": 'table',
    "schema": "mattermost"
  })
}}

WITH github_contributors AS (
    SELECT 
        author, 
        MIN(merged_at) AS min_contribution, 
        MAX(merged_at) AS max_contribution,
        SUM(*) AS total_contributions
    FROM {{ source('mattermost', 'github_contributions') }}
    GROUP BY 1
)

SELECT * FROM github_contributors

