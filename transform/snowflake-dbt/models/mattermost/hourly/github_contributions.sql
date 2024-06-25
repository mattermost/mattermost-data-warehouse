{{config({
    "materialized": 'table',
    "schema": "mattermost",
    "tags":["nightly"]
  })
}}

WITH github_all_contributors AS (
    SELECT 
        github_contributions_all.*
    FROM {{ source('staging', 'github_contributions_all') }}
)

SELECT * FROM github_all_contributors