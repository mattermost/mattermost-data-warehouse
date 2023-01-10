{{
    config(
        tags=['data-quality']
    )
}}

-- Thresholds percentage of missing servers.
{%- set threshold_pct_missing = 0.2 -%}

-- Checks if last week's missing  data are above given threshold
WITH cloud_stats AS (
    SELECT
        first_active_date::date AS date,
        COUNT(server_id) AS total,
        SUM(is_missing) AS total_missing,
        total_missing / total AS pct_missing
    FROM
        {{ref('dq_cloud_clearbit')}}
    WHERE
        -- Exclude not yet enriched
        is_enriched = 1
        -- Exclude users from most popular mail providers
        AND email_domain NOT IN (
            'gmail.com',
            'qq.com',
            'outlook.com',
            'googlemail.com',
            'hotmail.com',
            'yahoo.com',
            'protonmail.com',
            'icloud.com',
            'live.com',
            'mail.ru',
            'aol.com',
            'yandex.ru',
            'pdsb.net',
            'hotmail.co.uk',
            'me.com'
        )
    GROUP BY 1
)
SELECT
    *
FROM
    cloud_stats
WHERE
    date > DATEADD('day', -7, CURRENT_DATE)
    AND pct_missing > {{ threshold_pct_missing }}
