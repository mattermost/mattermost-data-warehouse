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
      , min(timestamp::DATE) AS min_nps_date
    FROM {{ source('mattermost_nps', 'nps_score') }} 
    GROUP BY 1, 2),

     dates                  AS (
         SELECT
             date_trunc('month', d.date) AS month
           , server_id
           , user_id
         FROM {{ source('util', 'dates') }}   d
              JOIN min_nps nps
                   ON d.date >= nps.min_nps_date
                       AND d.date <= date_trunc('month', current_date)
         GROUP BY 1, 2, 3
     ),

     max_date_by_month      AS (
         SELECT
             d.month
           , d.server_id
           , d.user_id
           , {{ dbt_utils.surrogate_key('d.month', 'd.server_id', 'd.user_id') }} as id
           , max(nps.timestamp)      AS max_timestamp
           , max(feedback.timestamp) AS max_feedback_timestamp
         FROM dates                                 d
              JOIN {{ source('mattermost_nps', 'nps_score') }}         nps
                   ON d.month >= date_trunc('month', nps.timestamp::DATE)
                       AND d.server_id = nps.server_id
                       AND d.user_id = nps.user_actual_id
              LEFT JOIN {{ source('mattermost_nps', 'nps_feedback') }} feedback
                        ON d.month >= date_trunc('month', feedback.timestamp::DATE)
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
           , nps.score
           , m.max_timestamp::DATE                              AS score_submission_date
           , nps.license_id
           , nps.license_sku
           , CASE WHEN nps.score < 7 THEN 'Detractor'
                  WHEN nps.score < 9 THEN 'Passive'
                  ELSE 'Promoter' END                           AS promoter_type
           , to_timestamp(nps.user_create_at / 1000)::DATE      AS user_created_at
           , to_timestamp(nps.server_install_date / 1000)::DATE AS server_install_date
           , feedback.feedback
           , m.max_feedback_timestamp::DATE                     AS feedback_submission_date
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
     )
SELECT *
FROM nps_user_monthly_score