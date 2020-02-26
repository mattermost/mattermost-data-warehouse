{{config({
    "materialized": "table",
    "schema": "ga"
  })
}}

WITH daily_page_visits AS (
    SELECT 
        'mattermost.com' AS site, 
        start_date,
        end_date,
        pagepath,
        pagetitle,
        pageviews,
        uniquepageviews,
        avgtimeonpage
    FROM {{ source('ga_mattermost_com_pages_visits', 'report') }}
    UNION ALL
    SELECT 
        'developers.mattermost.com' AS site, 
        start_date,
        end_date,
        pagepath,
        pagetitle,
        pageviews,
        uniquepageviews,
        avgtimeonpage
    FROM {{ source('ga_developers_pages_visits', 'report') }}
)
SELECT * FROM daily_page_visits