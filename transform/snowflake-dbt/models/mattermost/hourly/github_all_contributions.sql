{{config({
    "materialized": 'table',
    "schema": "mattermost"
  })
}}

WITH github_all_contributors AS (
    SELECT 
        github_contributions_all.*
    FROM {{ source('mattermost', 'github_contributions_all') }}
)

SELECT * FROM github_all_contributors