{{config({
    "materialized": 'incremental',
    "unique_key": 'id',
    "schema": "mattermost"
  })
}}

WITH min_nps                AS (
    SELECT
        server_id
      , user_actual_id       AS user_id
      , MIN(timestamp::DATE) AS min_nps_date
    FROM {{ source('mattermost_nps', 'nps_score') }} 
    GROUP BY 1, 2),

     dates                  AS (
         SELECT
             DATE_TRUNC('month', d.date) AS month
           , server_id
           , user_id
         FROM {{ source('util', 'dates') }}   d
              JOIN min_nps nps
                   ON d.date >= nps.min_nps_date
                       AND date_trunc('month', d.date) <= date_trunc('month', current_date)
         GROUP BY 1, 2, 3
     ),

     max_date_by_month      AS (
         SELECT
             d.month
           , d.server_id
           , d.user_id
           , {{ dbt_utils.surrogate_key('d.month', 'd.server_id', 'd.user_id') }} AS id
           , MAX(nps.timestamp)                                                   AS max_timestamp
           , MAX(DATE_TRUNC('month', feedback.timestamp::DATE))                   AS max_feedback_month
           , COUNT(DISTINCT nps.id)                                               AS responses_alltime
           , COUNT(DISTINCT CASE WHEN nps.score > 8 then nps.id else null end)    AS promoter_responses_alltime
           , COUNT(DISTINCT CASE WHEN nps.score < 7 then nps.id else null end)    AS detractor_responses_alltime
           , COUNT(DISTINCT CASE WHEN date_trunc('month', nps.timestamp::date) = d.month THEN nps.id
                        ELSE NULL END)                                            AS responses
           , COUNT(DISTINCT CASE WHEN date_trunc('month', nps.timestamp::date) = d.month 
                        AND nps.score > 8 THEN nps.id ELSE NULL END)              AS promoter_responses
           , COUNT(DISTINCT CASE WHEN date_trunc('month', nps.timestamp::date) = d.month 
                        AND nps.score < 7 THEN nps.id ELSE NULL END)              AS detractor_responses
           , COUNT(DISTINCT feedback.id)                                          AS feedback_count_alltime
           , COUNT(DISTINCT CASE WHEN DATE_TRUNC('month', feedback.timestamp) = d.month then feedback.id
                        ELSE NULL END)                                            AS feedback_count
         FROM dates                                 d
              JOIN {{ source('mattermost_nps', 'nps_score') }}         nps
                   ON d.month >= DATE_TRUNC('month', nps.timestamp::DATE)
                       AND d.server_id = nps.server_id
                       AND d.user_id = nps.user_actual_id
              LEFT JOIN {{ source('mattermost_nps', 'nps_feedback') }} feedback
                        ON d.month >= DATE_TRUNC('month', feedback.timestamp::DATE)
                            AND d.server_id = feedback.server_id
                            AND d.user_id = feedback.user_actual_id
         GROUP BY 1, 2, 3, 4
     ),

     nps_user_monthly_score AS (
         SELECT
             m.month
           , m.server_id
           , m.user_id
           , nps.user_role
           , nps.server_version
           , MAX(nps.score)                                                                    AS score
           , m.max_timestamp::DATE                                                             AS last_score_date
           , nps.license_id
           , nps.license_sku
           , CASE WHEN MAX(nps.score) < 7 THEN 'Detractor'
                  WHEN MAX(nps.score) < 9 THEN 'Passive'
                  ELSE 'Promoter' END                                                          AS promoter_type
           , to_timestamp(nps.user_create_at / 1000)::DATE                                     AS user_created_at
           , to_timestamp(nps.server_install_date / 1000)::DATE                                AS server_install_date
           , LIST_AGG(feedback.feedback, '; ') WITHIN GROUP (ORDER BY feedback.timestamp DESC) AS feedback
           , MAX(feedback.timestamp::DATE)                                                     AS last_feedback_date                        
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
              JOIN {{ source('mattermost_nps', 'nps_score') }}         nps
                   ON m.server_id = nps.server_id
                       AND m.user_id = nps.user_actual_id
                       AND m.max_timestamp = nps.timestamp
              LEFT JOIN {{ source('mattermost_nps', 'nps_feedback') }} feedback
                        ON m.server_id = feedback.server_id
                            AND m.user_id = feedback.user_actual_id
                            AND m.max_feedback_month = DATE_TRUNC('month', feedback.timestamp::DATE)
          {% if is_incremental() %}

          WHERE m.month >= (SELECT MAX(month) FROM {{this}})

          {% endif %}
          GROUP BY 1, 2, 3, 4, 5, 7, 8, 9, 11, 12, 15, 16, 17, 18, 19, 20, 21, 22, 23
     )
SELECT *
FROM nps_user_monthly_score