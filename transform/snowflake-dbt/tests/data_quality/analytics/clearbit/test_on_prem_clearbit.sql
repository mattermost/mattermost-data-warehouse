{{
    config(
        tags=['data-quality']
    )
}}

-- Thresholds percentage of missing servers.
{%- set threshold_pct_missing = 0.2 -%}

-- Checks if last week's missing activity data are above given threshold
WITH onprem_stats AS (
    SELECT
            first_active_date::date AS date,
            COUNT(server_id) AS total,
            SUM(is_missing) AS total_missing,
            SUM(has_exception) AS total_exceptions,
            total_missing / total AS pct_missing
        FROM
            {{ref('dq_on_prem_clearbit')}}
        GROUP BY 1
)
SELECT
    *
FROM
    onprem_stats
WHERE
    date > DATEADD('day', -7, CURRENT_DATE)
    AND pct_missing > {{ threshold_pct_missing }}