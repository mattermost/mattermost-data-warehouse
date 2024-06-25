{{config({
    "materialized": 'table',
    "schema": "mattermost",
    "tags":["nightly"]
  })
}}

WITH github_all_contributors AS (
    SELECT 
        author, 
        MIN(merged_at) AS min_contribution, 
        MAX(merged_at) AS max_contribution,
        COUNT(*) AS total_contributions
    FROM {{ source('staging', 'github_contributions_all') }}
    GROUP BY 1
)

SELECT * FROM github_all_contributors
