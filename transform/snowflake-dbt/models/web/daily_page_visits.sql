{{config({
    "materialized": "table",
    "schema": "web"
  })
}}

WITH daily_page_visits AS (
    SELECT 
        'mattermost.com' AS site, 
        start_date,
        end_date,
        pagepath,
        pagetitle,
        sum(uniquepageviews) as uniquepageviews,
        sum(pageviews) as pageviews
    FROM {{ source('ga_mattermost_com_pages_visits', 'report') }}
    GROUP BY 1,2,3,4,5
    UNION ALL
    SELECT 
        'developers.mattermost.com' AS site, 
        start_date,
        end_date,
        pagepath,
        pagetitle,
        sum(uniquepageviews),
        sum(pageviews)
    FROM {{ source('ga_developers_pages_visits', 'report') }}
    GROUP BY 1,2,3,4,5
    UNION ALL
    SELECT 
        'handbook.mattermost.com' AS site, 
        start_date,
        end_date,
        split_part(pagepath,'/@mattermost/s/handbook',2),
        pagetitle,
        sum(uniquepageviews),
        sum(pageviews)
    FROM {{ source('ga_mattermost_handbook_pages_visits', 'report') }}
    GROUP BY 1,2,3,4,5
    UNION ALL
    SELECT 
        'docs.mattermost.com' AS site, 
        start_date,
        end_date,
        pagepath,
        pagetitle,
        sum(uniquepageviews),
        sum(pageviews)
    FROM {{ source('ga_docs_pages_visits_sources', 'report') }}
    GROUP BY 1,2,3,4,5
    UNION ALL
    SELECT 
        'licensing.mattermost.com' AS site, 
        start_date,
        end_date,
        pagepath,
        pagetitle,
        sum(uniquepageviews),
        sum(pageviews)
    FROM {{ source('ga_licensing_pages_visits', 'report') }}
    GROUP BY 1,2,3,4,5

    
)
SELECT * FROM daily_page_visits