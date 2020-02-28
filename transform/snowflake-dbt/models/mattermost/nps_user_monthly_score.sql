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
           , MAX(feedback.timestamp)                                              AS max_feedback_timestamp
           , COUNT(nps.user_actual_id)                                            AS responses_alltime
           , COUNT(CASE WHEN date_trunc('month', nps.timestamp::date) = d.month THEN nps.user_actual_id
                        ELSE NULL END)                                            AS responses 
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
           , MAX(nps.score)                                     AS score
           , m.max_timestamp::DATE                              AS score_submission_date
           , nps.license_id
           , nps.license_sku
           , CASE WHEN MAX(nps.score) < 7 THEN 'Detractor'
                  WHEN MAX(nps.score) < 9 THEN 'Passive'
                  ELSE 'Promoter' END                           AS promoter_type
           , to_timestamp(nps.user_create_at / 1000)::DATE      AS user_created_at
           , to_timestamp(nps.server_install_date / 1000)::DATE AS server_install_date
           , feedback.feedback
           , m.max_feedback_timestamp::DATE                     AS feedback_submission_date
           , m.responses_alltime                          
           , m.responses
           , m.id
         FROM max_date_by_month                     m
              JOIN {{ source('mattermost_nps', 'nps_score') }}         nps
                   ON m.server_id = nps.server_id
                       AND m.user_id = nps.user_actual_id
                       AND m.max_timestamp = nps.timestamp
              LEFT JOIN {{ source('mattermost_nps', 'nps_feedback') }} feedback
                        ON m.server_id = feedback.server_id
                            AND m.user_id = feedback.user_actual_id
                            AND m.max_feedback_timestamp = feedback.timestamp
          {% if is_incremental() %}

          WHERE m.month >= (SELECT MAX(month) FROM {{this}})

          {% endif %}
          GROUP BY 1, 2, 3, 4, 5, 7, 8, 9, 11, 12, 13, 14, 14, 15, 16, 17
     )
SELECT *
FROM nps_user_monthly_score