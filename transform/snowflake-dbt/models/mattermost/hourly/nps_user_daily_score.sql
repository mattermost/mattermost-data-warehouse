{{config({
    "materialized": 'incremental',
    "unique_key": 'id',
    "schema": "mattermost"
  })
}}

WITH daily_nps_scores AS (
    
        SELECT timestamp::date as date
        , timestamp::timestamp as timestamp
        , id
  		  , license_id
        , server_version
        , user_role
        , server_install_date
        , license_sku
        , user_create_at
        , score
        , user_actual_id
        , user_id as server_id
  	FROM (
          SELECT ROW_NUMBER() over (PARTITION BY timestamp::DATE, user_id ORDER BY timestamp DESC) AS rownum, *
          FROM {{ source('mattermost_nps', 'nps_score') }}
          WHERE TIMESTAMP <= CURRENT_TIMESTAMP
          {% if is_incremental() %}
           AND TIMESTAMP::date >= (SELECT MAX(date) from {{this}})
          {% endif %}
      )
  	where rownum = 1

        UNION ALL
    
        SELECT 
            original_timestamp::date as date
            , original_timestamp::timestamp as timestamp
            , id
            , license_id
            , serverversion as server_version
            , user_role
            , server_install_date
            , license_sku
            , user_create_at
            , score
            , useractualid as user_actual_id
            , user_id as server_id
        FROM (
            SELECT ROW_NUMBER() over (PARTITION BY original_timestamp::DATE, user_id ORDER BY original_timestamp DESC) AS rownum, *
            FROM {{ source('mm_plugin_prod', 'nps_nps_score') }}
            WHERE ORIGINAL_TIMESTAMP::timestamp <= CURRENT_TIMESTAMP
            {% if is_incremental() %}
            AND ORIGINAL_TIMESTAMP::date >= (SELECT MAX(date) from {{this}})
            {% endif %}
        )
        where rownum = 1
        
), 

min_nps                AS (
    SELECT
        server_id
      , user_actual_id       AS user_id
      , MIN(timestamp::DATE) AS min_nps_date
    FROM daily_nps_scores
    WHERE TIMESTAMP <= CURRENT_TIMESTAMP 
    GROUP BY 1, 2),

     dates                  AS (
         SELECT
             d.date AS date
           , server_id
           , user_id
         FROM {{ source('util', 'dates') }}   d
              JOIN min_nps nps
                   ON d.date >= nps.min_nps_date
                       AND d.date <= current_date
         GROUP BY 1, 2, 3
     ),

     daily_feedback_scores AS (
    
        SELECT
            timestamp::date as date
          , timestamp::timestamp AS timestamp
          , id
          , user_id as server_id
          , user_actual_id as user_id
          , feedback
  	FROM (
          SELECT ROW_NUMBER() over (PARTITION BY timestamp::DATE, user_id ORDER BY timestamp DESC) AS rownum, *
          FROM {{ source('mattermost_nps', 'nps_feedback') }}
          WHERE TIMESTAMP <= CURRENT_TIMESTAMP
            {% if is_incremental() %}
            AND TIMESTAMP::date >= (SELECT MAX(date) from {{this}})
            {% endif %}
      )
  	where rownum = 1
    
    UNION ALL
    
        SELECT
            original_timestamp::date as date
          , original_timestamp::timestamp as timestamp
          , id
          , user_id as server_id
          , useractualid as user_id
          , feedback
        FROM (
                SELECT ROW_NUMBER() over (PARTITION BY original_timestamp::DATE, user_id ORDER BY original_timestamp DESC) AS rownum, *
                FROM {{ source('mm_plugin_prod', 'nps_nps_feedback') }} 
                WHERE ORIGINAL_TIMESTAMP::timestamp <= CURRENT_TIMESTAMP
            {% if is_incremental() %}
            AND ORIGINAL_TIMESTAMP::date >= (SELECT MAX(date) from {{this}})
            {% endif %}
        )
        WHERE rownum = 1
    
), 

     max_date_by_month      AS (
         SELECT
             d.date
           , d.server_id
           , d.user_id
           , {{ dbt_utils.surrogate_key('d.date', 'd.server_id', 'd.user_id') }}  AS id
           , MAX(nps.timestamp)                                                   AS max_timestamp
           , MAX(date_trunc('day', feedback.timestamp::DATE))                     AS max_feedback_date
           , COUNT(DISTINCT nps.id)                                               AS responses_alltime
           , COUNT(DISTINCT CASE WHEN nps.score > 8 then nps.id else null end)    AS promoter_responses_alltime
           , COUNT(DISTINCT CASE WHEN nps.score < 7 then nps.id else null end)    AS detractor_responses_alltime
           , COUNT(DISTINCT CASE WHEN date_trunc('day', nps.timestamp::date) = d.date THEN nps.id
                        ELSE NULL END)                                            AS responses
           , COUNT(DISTINCT CASE WHEN date_trunc('day', nps.timestamp::date) = d.date 
                        AND nps.score > 8 THEN nps.id ELSE NULL END)              AS promoter_responses
           , COUNT(DISTINCT CASE WHEN date_trunc('day', nps.timestamp::date) = d.date 
                        AND nps.score < 7 THEN nps.id ELSE NULL END)              AS detractor_responses
           , COUNT(DISTINCT feedback.id)                                          AS feedback_count_alltime
           , COUNT(DISTINCT CASE WHEN DATE_TRUNC('day', feedback.timestamp) = d.date then feedback.id
                        ELSE NULL END)                                            AS feedback_count
         FROM dates                                 d
              JOIN daily_nps_scores         nps
                   ON d.date >= nps.timestamp::DATE
                       AND d.server_id = nps.server_id
                       AND d.user_id = nps.user_actual_id
              LEFT JOIN daily_feedback_scores feedback
                        ON d.date >= feedback.timestamp::DATE
                            AND d.server_id = feedback.server_id
                            AND d.user_id = feedback.user_id
         GROUP BY 1, 2, 3, 4
     ),

     nps_user_daily_score AS (
         SELECT
             m.date
           , m.server_id
           , m.user_id
           , nps.user_role
           , nps.server_version
           , MAX(nps.score)                                                                       AS score
           , m.max_timestamp::DATE                                                                AS last_score_date
           , nps.license_id
           , nps.license_sku
           , CASE WHEN MAX(nps.score) < 7 THEN 'Detractor'
                  WHEN MAX(nps.score) < 9 THEN 'Passive'
                  ELSE 'Promoter' END                                                             AS promoter_type
           , to_timestamp(nps.user_create_at / 1000)::DATE                                        AS user_created_at
           , to_timestamp(nps.server_install_date / 1000)::DATE                                   AS server_install_date
           , LISTAGG(DISTINCT feedback.feedback, '; ') WITHIN GROUP (ORDER BY feedback.feedback)  AS feedback
           , MAX(feedback.timestamp::DATE)                                                        AS last_feedback_date                        
           , m.responses
           , m.promoter_responses
           , m.detractor_responses
           , m.responses_alltime
           , m.promoter_responses_alltime
           , m.detractor_responses_alltime
           , m.feedback_count
           , m.feedback_count_alltime
           , m.id
         FROM max_date_by_month                     m
              JOIN daily_nps_scores         nps
                   ON m.server_id = nps.server_id
                       AND m.user_id = nps.user_actual_id
                       AND m.max_timestamp = nps.timestamp
              LEFT JOIN daily_feedback_scores feedback
                        ON m.server_id = feedback.server_id
                            AND m.user_id = feedback.user_id
                            AND m.max_feedback_date = DATE_TRUNC('day', feedback.timestamp::DATE)
          {% if is_incremental() %}

          WHERE m.date >= (SELECT MAX(date) FROM {{this}})

          {% endif %}
          GROUP BY 1, 2, 3, 4, 5, 7, 8, 9, 11, 12, 15, 16, 17, 18, 19, 20, 21, 22, 23
     )
SELECT *
FROM nps_user_daily_score