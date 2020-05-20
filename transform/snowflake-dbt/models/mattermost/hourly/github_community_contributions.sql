{{config({
    "materialized": 'table',
    "schema": "mattermost"
  })
}}

WITH github_community_contributors AS (
    SELECT 
        github_contributions_all.*
    FROM {{ source('mattermost', 'github_contributions_all') }}
    LEFT JOIN {{ source('mattermost', 'github_repo_categorization')}} ON github_contributions_all.repo = github_repo_categorization.repo
    LEFT JOIN {{ source('employee', 'staff_github_usernames')}} ON github_contributions_all.author = staff_github_usernames.username
    WHERE github_repo_categorization.repo IN ('community','plugin') AND staff_github_usernames.username IS NULL
)

SELECT * FROM github_community_contributors