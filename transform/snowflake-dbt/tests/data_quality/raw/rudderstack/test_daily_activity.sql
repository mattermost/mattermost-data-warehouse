{{
    config(
        tags=['data-quality']
    )
}}

-- Thresholds for cloud, on prem and total missing data
{%- set min_pct_missing_cloud = 0.01 -%}
{%- set min_pct_missing_onprem = 0.01 -%}
{%- set min_pct_missing = 0.01 -%}

-- Checks if yesterday's missing activity data are above given threshold
WITH activity_stats AS (
    SELECT
        date,
        SUM(CASE WHEN is_missing AND is_cloud THEN 1 ELSE 0 END) AS cnt_missing_cloud,
        SUM(CASE WHEN is_missing AND is_cloud THEN 0 ELSE 1 END) AS cnt_existing_cloud,
        SUM(CASE WHEN is_missing AND not is_cloud THEN 1 ELSE 0 END) AS cnt_missing_onprem,
        SUM(CASE WHEN is_missing AND not is_cloud THEN 0 ELSE 1 END) AS cnt_existing_onprem,
        cnt_missing_cloud*1.0 / (cnt_missing_cloud + cnt_existing_cloud) AS pct_missing_cloud,
        cnt_missing_onprem*1.0 / (cnt_missing_onprem + cnt_existing_onprem) AS pct_missing_onprem,
        (cnt_missing_cloud + cnt_missing_onprem)*1.0 / (cnt_missing_cloud + cnt_missing_onprem + cnt_existing_cloud + cnt_existing_onprem) AS pct_missing,
        cnt_missing_cloud + cnt_missing_onprem + cnt_existing_cloud + cnt_existing_onprem AS total
    FROM
        {{ ref('dq_daily_activity') }}
    GROUP BY 1
    ORDER BY 1
)
SELECT
    *
FROM
    activity_stats
WHERE
    date > DATEADD('day', -7, CURRENT_DATE)
    AND (
        pct_missing_cloud > {{ min_pct_missing_cloud }}
        OR pct_missing_onprem > {{ min_pct_missing_onprem }}
        OR pct_missing > {{ min_pct_missing }}
    )