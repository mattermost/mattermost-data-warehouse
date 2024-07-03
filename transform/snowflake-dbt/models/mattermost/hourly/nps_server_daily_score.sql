{{config({
    "materialized": 'incremental',
    "unique_key": 'id',
    "schema": "mattermost",
    "tags":["nightly"]
  })
}}

WITH nps_server_daily_score AS (
    SELECT
        date
      , server_id
      , {{ dbt_utils.surrogate_key(['date', 'server_id']) }}                                                    AS id
      , MAX(server_version)                                                                                   AS server_version
      , MIN(server_install_date)                                                                              AS server_install_date
      , COUNT(DISTINCT CASE WHEN score > 8 THEN user_id ELSE NULL END)                                        AS promoters
      , COUNT(DISTINCT CASE WHEN score <= 6 THEN user_id ELSE NULL END)                                       AS detractors
      , COUNT(DISTINCT
              CASE WHEN score > 6 AND score <= 8 THEN user_id ELSE NULL END)                                  AS passives
      , COUNT(DISTINCT user_id)                                                                               AS nps_users
      , AVG(score)                                                                                            AS avg_score
      , 100.0 * ((count(DISTINCT CASE WHEN score > 8 THEN user_id ELSE NULL END) / count(DISTINCT user_id)) -
                 (count(DISTINCT CASE WHEN score <= 6 THEN user_id ELSE NULL END) /
                  count(DISTINCT user_id)))                                                                   AS nps_score
      , SUM(responses)                                                                                        AS responses
      , SUM(responses_alltime)                                                                                AS responses_alltime
      , LISTAGG(DISTINCT feedback) WITHIN GROUP (ORDER BY feedback)                                           AS feedback
      , SUM(feedback_count)                                                                                   AS feedback_responses
      , SUM(feedback_count_alltime)                                                                           AS feedback_responses_alltime
    FROM {{ ref('nps_user_daily_score') }}
    {% if is_incremental() %}

    WHERE date >= (SELECT MAX(date) FROM {{this}})

    {% endif %}
    GROUP BY 1, 2
                                 )
SELECT *
FROM nps_server_daily_score