{{config({
    "materialized": 'table',
    "schema": "data_quality",
    "tags":'data-quality'
  })
}}

-- Number of days to keep activity data for monitoring
{% set retention = 90 %}

WITH server_activities AS (
    -- Find the dates where a server has been sending activity data
    SELECT
        USER_ID AS server_id,
        TIMESTAMP::DATE AS date,
        COUNT(*) AS cnt_activities
    FROM
        {{ source('mm_telemetry_prod', 'activity') }}
    WHERE
        TIMESTAMP BETWEEN DATEADD('day', -{{ retention }}, CURRENT_TIMESTAMP) AND CURRENT_TIMESTAMP
    GROUP BY 1, 2
),
server_activity_range AS (
    -- Find the date range where each server has been sending data
    SELECT
        server_id,
        MIN(date) AS start_date,
        MAX(date) AS end_date
    FROM
        server_activities
    GROUP BY 1
),
dates AS (
    -- Find all dates between start and today
    SELECT
        DISTINCT(TIMESTAMP::DATE) AS date
    FROM
        {{ source('mm_telemetry_prod', 'activity') }}
    WHERE
        TIMESTAMP BETWEEN DATEADD('day', -{{ retention }}, CURRENT_TIMESTAMP) AND CURRENT_TIMESTAMP
),
expected_dates AS (
    -- Find all dates with expected activity data for a given server
    SELECT
        sar.server_id, date
    FROM
        server_activity_range sar
        CROSS JOIN dates d
    WHERE
        d.date BETWEEN sar.start_date AND sar.end_date
)
SELECT
    {{ dbt_utils.surrogate_key(['sd.server_id', 'sd.date']) }} as daily_activity_id,
    sd.server_id,
    sd.date,
    COALESCE(sa.cnt_activities, 0) AS cnt_activities,
    CASE WHEN sa.server_id IS NULL THEN true ELSE false END AS is_missing,
    CASE WHEN sf.INSTALLATION_ID IS NOT NULL THEN true ELSE false END AS is_cloud
FROM
    expected_dates sd
    LEFT JOIN server_activities sa ON sd.server_id = sa.server_id AND sd.date = sa.date
    LEFT JOIN {{ ref('server_fact') }} sf ON sd.server_id = sf.server_id

