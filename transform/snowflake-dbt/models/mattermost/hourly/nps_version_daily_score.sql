{{config({
    "materialized": 'incremental',
    "unique_key": 'id',
    "schema": "mattermost"
  })
}}

WITH min_nps                AS (
    SELECT
        server_version
      , MIN(timestamp::DATE) AS min_nps_date
    FROM {{ source('mattermost_nps', 'nps_score') }} 
    GROUP BY 1),

     dates                  AS (
         SELECT
             d.date AS date
           , server_version
         FROM {{ source('util', 'dates') }}   d
              JOIN min_nps nps
                   ON d.date >= nps.min_nps_date
                       AND d.date <= current_date
         GROUP BY 1, 2
     ),

     nps_version_daily_score      AS (
         SELECT
             d.date
           , d.server_version
           , {{ dbt_utils.surrogate_key('d.date', 'd.server_version') }}  AS id
           , MAX(nps.timestamp)                                                   AS max_timestamp
           , COUNT(DISTINCT nps.id)                                               AS responses_alltime
           , COUNT(DISTINCT CASE WHEN nps.score > 8 then nps.id else null end)    AS promoter_responses_alltime
           , COUNT(DISTINCT CASE WHEN nps.score < 7 then nps.id else null end)    AS detractor_responses_alltime
           , COUNT(DISTINCT CASE WHEN date_trunc('day', nps.timestamp::date) = d.date THEN nps.id
                        ELSE NULL END)                                            AS responses
           , COUNT(DISTINCT CASE WHEN date_trunc('day', nps.timestamp::date) = d.date 
                        AND nps.score > 8 THEN nps.id ELSE NULL END)              AS promoter_responses
           , COUNT(DISTINCT CASE WHEN date_trunc('day', nps.timestamp::date) = d.date 
                        AND nps.score < 7 THEN nps.id ELSE NULL END)              AS detractor_responses
         FROM dates                                 d
              JOIN {{ source('mattermost_nps', 'nps_score') }}         nps
                   ON d.date >= nps.timestamp::DATE
                       AND d.server_version = nps.server_version
         GROUP BY 1, 2, 3
     )
SELECT *
FROM nps_version_daily_score