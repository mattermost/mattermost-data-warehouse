{{config({
    "materialized": "table",
    "schema": "ga"
  })
}}

WITH ga_daily_traffic AS (
    SELECT 
        'mattermost.com' AS site, 
        channelgrouping,
        start_date,
        end_date,
        source,
        users
    FROM {{ source('ga_channelgroupingsource_users_com', 'report') }}
    UNION ALL
    SELECT 
        'mattermost.org' AS site, 
        channelgrouping,
        start_date,
        end_date,
        source,
        users
    FROM {{ source('ga_channelgroupingsource_users_org', 'report') }}
)
SELECT * FROM ga_daily_traffic