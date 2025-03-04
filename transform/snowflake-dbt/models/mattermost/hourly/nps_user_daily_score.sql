{{config({
    "materialized": "incremental",
    "unique_key": "id",
    "on_schema_change": "append_new_columns",
    "schema": "mattermost",
    "tags": ["nightly"]
  })
}}

{% if is_incremental() %}
  WITH max_date AS (
    SELECT MAX(DATE) AS max_date
    FROM {{this}}
  ),

  daily_nps_scores AS (
{% else %}
WITH daily_nps_scores AS (
{% endif %}
    
    SELECT *
  	FROM {{ ref('nps_data') }}
        
), 

min_nps                AS (
    SELECT
        server_id
      , user_id       AS user_id
      , min(timestamp) AS min_nps_date
    FROM {{ ref('nps_data') }}
    WHERE TIMESTAMP::DATE <= CURRENT_DATE
    GROUP BY 1, 2),

     dates                  AS (
         SELECT
             d.date AS date
           , server_id
           , user_id
         FROM {{ ref('dates') }}   d
              JOIN min_nps nps
                   ON d.date >= nps.min_nps_date::date
                       AND d.date <= current_date
         GROUP BY 1, 2, 3
     ),
     max_date_by_month      AS (
         SELECT
             d.date
           , d.server_id
           , d.user_id
           , {{ dbt_utils.surrogate_key(['d.date', 'd.server_id', 'd.user_id']) }}      AS id
           , MAX(nps.timestamp)                                                       AS max_timestamp
           , MAX(nps.timestamp::DATE)                                                 AS max_feedback_date
           , COUNT(DISTINCT nps.nps_id)                                               AS responses_alltime
           , COUNT(DISTINCT CASE WHEN nps.score > 8 then nps.nps_id else null end)    AS promoter_responses_alltime
           , COUNT(DISTINCT CASE WHEN nps.score < 7 then nps.nps_id else null end)    AS detractor_responses_alltime
           , COUNT(DISTINCT CASE WHEN nps.timestamp::date = d.date THEN nps.nps_id
                        ELSE NULL END)                                                AS responses
           , COUNT(DISTINCT CASE WHEN nps.timestamp::date = d.date 
                        AND nps.score > 8 THEN nps.nps_id ELSE NULL END)              AS promoter_responses
           , COUNT(DISTINCT CASE WHEN nps.timestamp::date = d.date 
                        AND nps.score < 7 THEN nps.nps_id ELSE NULL END)              AS detractor_responses
           , COUNT(DISTINCT nps.feedback_id)                                          AS feedback_count_alltime
           , COUNT(DISTINCT CASE WHEN nps.timestamp::date = d.date then nps.feedback_id
                        ELSE NULL END)                                                AS feedback_count
         FROM dates                                 d
              JOIN daily_nps_scores         nps
                   ON d.date >= nps.timestamp::DATE
                       AND d.server_id = nps.server_id
                       AND d.user_id = nps.user_id
         {% if is_incremental() %}
           JOIN max_date
           ON d.date >= max_date.max_date
         {% endif %}
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
           , nps.user_create_at::DATE                                                             AS user_created_at
           , nps.server_install_date::DATE                                                        AS server_install_date
           , LISTAGG(DISTINCT nps.feedback, '; ') WITHIN GROUP (ORDER BY nps.feedback)            AS feedback
           , MAX(nps.timestamp::DATE)                                                             AS last_feedback_date                        
           , m.responses
           , m.promoter_responses
           , m.detractor_responses
           , m.responses_alltime
           , m.promoter_responses_alltime
           , m.detractor_responses_alltime
           , m.feedback_count
           , m.feedback_count_alltime
           , m.id
           , LISTAGG(DISTINCT nps.email, '; ') WITHIN GROUP (ORDER BY nps.email)                 AS email
         FROM max_date_by_month                     m
              JOIN daily_nps_scores         nps
                   ON m.server_id = nps.server_id
                       AND m.user_id = nps.user_id
                       AND m.max_timestamp = nps.timestamp
          {% if is_incremental() %}
              JOIN max_date
                  ON m.date >= max_date.max_date
         {% endif %}
          GROUP BY 1, 2, 3, 4, 5, 7, 8, 9, 11, 12, 15, 16, 17, 18, 19, 20, 21, 22, 23
     )
SELECT *
FROM nps_user_daily_score