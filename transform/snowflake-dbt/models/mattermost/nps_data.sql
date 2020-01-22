{{config({
    "materialized": 'table',
    "schema": "mattermost"
  })
}}

WITH nps_data AS (
    SELECT
       nps_score.license_id,
       nps_score.timestamp::DATE AS date,
       nps_score.server_version,
       nps_score.user_role,
       nps_score.server_install_date,
       nps_score.license_sku,
       nps_score.user_create_at,
       nps_score.score,
       CASE WHEN nps_score.score < 7 THEN 'Detractor' WHEN nps_score.score < 9 THEN 'Passive' ELSE 'Promoters' END AS promotor_type,
       nps_score.user_actual_id AS user_id,
       nps_score.user_id AS server_id,
       nps_feedback.feedback
	FROM {{ source('mattermost_nps', 'nps_score') }}
    LEFT JOIN {{ source('mattermost_nps', 'nps_feedback') }}
        ON nps_score.user_actual_id = nps_feedback.user_actual_id AND nps_score.timestamp::DATE = nps_feedback.timestamp::DATE
)

SELECT * FROM nps_data