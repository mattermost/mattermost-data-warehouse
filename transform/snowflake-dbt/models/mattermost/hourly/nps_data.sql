{{config({
    "materialized": 'table',
    "schema": "mattermost"
  })
}}

WITH daily_nps_scores AS (
    
        SELECT timestamp::date as date
  		, license_id
        , server_version
        , user_role
        , server_install_date
        , license_sku
        , user_create_at
        , score
        , user_actual_id
        , user_id
  	FROM (
          SELECT ROW_NUMBER() over (PARTITION BY timestamp::DATE, user_id ORDER BY timestamp DESC) AS rownum, *
          FROM {{ source('mattermost_nps', 'nps_score') }}
      )
  	where rownum = 1

        UNION ALL
    
        SELECT 
            timestamp::date as date
            , license_id
            , serverversion as server_version
            , user_role
            , server_install_date
            , license_sku
            , user_create_at
            , score
            , useractualid as user_actual_id
            , user_id
        FROM (
            SELECT ROW_NUMBER() over (PARTITION BY timestamp::DATE, user_id ORDER BY timestamp DESC) AS rownum, *
            FROM {{ source('mm_plugin_prod', 'nps_nps_score') }}
        )
        where rownum = 1
        
), 

daily_feedback_scores AS (
    
        SELECT
            timestamp::date as date
          , user_actual_id
          , feedback
  	FROM (
          SELECT ROW_NUMBER() over (PARTITION BY timestamp::DATE, user_id ORDER BY timestamp DESC) AS rownum, *
          FROM {{ source('mattermost_nps', 'nps_feedback') }}
      )
  	where rownum = 1
    
    UNION ALL
    
        SELECT
            timestamp::date as date
          , useractualid as user_actual_id
          , feedback
        FROM (
                SELECT ROW_NUMBER() over (PARTITION BY timestamp::DATE, user_id ORDER BY timestamp DESC) AS rownum, *
                FROM {{ source('mm_plugin_prod', 'nps_nps_feedback') }} 
        )
        WHERE rownum = 1
    
), nps_data AS (
    SELECT
       daily_nps_scores.license_id,
       daily_nps_scores.date,
       daily_nps_scores.server_version,
       daily_nps_scores.user_role,
       to_timestamp(daily_nps_scores.server_install_date/1000)::DATE AS server_install_date,
       daily_nps_scores.license_sku,
       to_timestamp(daily_nps_scores.user_create_at/1000)::DATE AS user_create_at,
       daily_nps_scores.score,
       CASE WHEN daily_nps_scores.score < 7 THEN 'Detractor' WHEN daily_nps_scores.score < 9 THEN 'Passive' ELSE 'Promoter' END AS promoter_type,
       daily_nps_scores.user_actual_id AS user_id,
       daily_nps_scores.user_id AS server_id,
       daily_feedback_scores.feedback
	FROM daily_nps_scores
    LEFT JOIN daily_feedback_scores
        ON daily_nps_scores.user_actual_id = daily_feedback_scores.user_actual_id AND daily_nps_scores.date = daily_feedback_scores.date
)

SELECT * FROM nps_data