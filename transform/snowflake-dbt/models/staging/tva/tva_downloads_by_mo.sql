{{config({
    "materialized": 'table',
    "schema": "staging"
  })
}}

WITH actual_downloads_by_mo AS (
    SELECT
        DATE_TRUNC('month', downloads.logdate) AS month,
	    COUNT(*) AS download_count
    FROM {{ ref('downloads')}}
    GROUP BY 1
), tva_downloads_by_mo AS (
    SELECT
        'downloads_by_mo' as target_slug,
        downloads_by_mo.month,
        downloads_by_mo.month + interval '1 month' - interval '1 day' AS period_last_day,
        downloads_by_mo.target,
        actual_downloads_by_mo.download_count AS actual,
        round((actual_downloads_by_mo.download_count/downloads_by_mo.target),2) AS tva
    FROM {{ source('targets', 'downloads_by_mo') }}
    LEFT JOIN actual_downloads_by_mo ON downloads_by_mo.month = actual_downloads_by_mo.month
)

SELECT * FROM tva_downloads_by_mo