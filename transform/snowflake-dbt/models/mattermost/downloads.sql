{{config({
    "materialized": 'incremental',
    "schema": "mattermost"
  })
}}

WITH downloads AS (
    SELECT
        logdate::date as logdate,
        cip,
        CASE
            WHEN uri LIKE '%desktop%' THEN 'Desktop'
            WHEN uri LIKE '%team%' and uri NOT LIKE '%appcenter%' THEN 'TE'
            WHEN uri NOT LIKE '%team%' and uri NOT LIKE '%appcenter%' THEN 'EE'
            else '?'
        END AS type,
    	COUNT(*) AS count,
    	MAX(CASE
    	    WHEN uri LIKE '%desktop%' THEN split_part(uri, '/', 3)
    	    ELSE split_part(uri, '/', 2)
    	END) AS version,
    	MAX(useragent) AS useragent
    FROM {{ source('releases', 'log_entries') }} 
    WHERE ((uri LIKE '%desktop%')
            OR (
                uri LIKE '/6.%'
                OR uri LIKE '/5.%'
                OR uri LIKE '/4.%'
                OR uri LIKE '/3.%'
                OR uri LIKE '/2.%'
                OR uri LIKE '/1.%')
            )
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        AND logdate > (SELECT MAX(logdate) FROM {{ this }})

    {% endif %}
    GROUP BY logdate, cip, type
)

SELECT * FROM downloads