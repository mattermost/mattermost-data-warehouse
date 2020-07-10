{{config({
    "materialized": 'table',
    "schema": "mattermost"
  })
}}

WITH github_community_contributors AS (
    SELECT 
        author, 
        MIN(merged_at) AS min_contribution, 
        MAX(merged_at) AS max_contribution,
        COUNT(*) AS total_contributions
    FROM {{ source('staging', 'github_contributions_all') }}
    LEFT JOIN {{ source('mattermost', 'github_repo_categorization')}} ON github_contributions_all.repo = github_repo_categorization.repo
    LEFT JOIN {{ source('employee', 'staff_github_usernames')}} ON github_contributions_all.author = staff_github_usernames.username
    WHERE github_repo_categorization.is_community = 'true'
    GROUP BY 1
)

SELECT * FROM github_community_contributors

