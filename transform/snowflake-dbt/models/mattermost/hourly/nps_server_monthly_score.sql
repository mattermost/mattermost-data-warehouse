{{config({
    "materialized": 'incremental',
    "unique_key": 'id',
    "schema": "mattermost"
  })
}}

WITH nps_server_monthly_score AS (
    SELECT
        month
      , server_id
      , {{ dbt_utils.surrogate_key('d.month', 'd.server_id') }} AS id
      , count(DISTINCT CASE WHEN score > 8 THEN user_id ELSE NULL END)                                        AS promoters
      , count(DISTINCT CASE WHEN score <= 6 THEN user_id ELSE NULL END)                                       AS detractors
      , count(DISTINCT
              CASE WHEN score > 6 AND score <= 8 THEN user_id ELSE NULL END)                                  AS passives
      , count(DISTINCT user_id)                                                                               AS nps_users
      , avg(score)                                                                                            AS avg_score
      , 100.0 * ((count(DISTINCT CASE WHEN score > 8 THEN user_id ELSE NULL END) / count(DISTINCT user_id)) -
                 (count(DISTINCT CASE WHEN score <= 6 THEN user_id ELSE NULL END) /
                  count(DISTINCT user_id)))                                                                   AS nps_score
    FROM {{ ref('nps_user_monthly_score') }}
    {% if is_incremental() %}

    WHERE month >= (SELECT MAX(month) FROM {{this}})

    {% endif %}
    GROUP BY 1, 2
                                 )
SELECT *
FROM nps_server_monthly_score