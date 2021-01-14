{{config({
    "materialized": 'incremental',
    "schema": "mattermost",
    "unique_key":'id'
  })
}}

{% if is_incremental() %}
WITH max_time AS (
    SELECT MAX(TIMESTAMP) - INTERVAL '6 HOURS' AS MAX_TIME
    FROM {{ this }}
),

daily_nps_scores AS (
{% else %}
WITH daily_nps_scores AS (
{% endif %}
    
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
        , timestamp::timestamp as timestamp
        , id as nps_id
  	FROM (
          SELECT ROW_NUMBER() over (PARTITION BY nps.timestamp::DATE, nps.user_id ORDER BY nps.timestamp DESC) AS rownum, nps.*
          FROM {{ source('mattermost_nps', 'nps_score') }} nps
          {% if is_incremental() %}
          JOIN max_time mt 
          ON nps.TIMESTAMP >= mt.max_time
          {% endif %}
          WHERE nps.timestamp::DATE <= CURRENT_DATE
      )
  	where rownum = 1

        UNION ALL
    
        SELECT 
            original_timestamp::date as date
            , license_id
            , serverversion as server_version
            , user_role
            , server_install_date
            , license_sku
            , user_create_at
            , score
            , useractualid as user_actual_id
            , user_id
            , timestamp::timestamp as timestamp
            , id as nps_id
        FROM (
            SELECT ROW_NUMBER() over (PARTITION BY nps.timestamp::DATE, nps.user_id ORDER BY nps.timestamp DESC) AS rownum, nps.*
            FROM {{ source('mm_plugin_prod', 'nps_nps_score') }} nps
            {% if is_incremental() %}
            JOIN max_time mt 
            ON nps.TIMESTAMP >= mt.max_time
            {% endif %}
            WHERE nps.TIMESTAMP::date <= CURRENT_DATE
        )
        where rownum = 1
        
), 

daily_feedback_scores AS (
    
        SELECT
            timestamp::date as date
          , user_actual_id
          , feedback
          , id as feedback_id
  	FROM (
          SELECT ROW_NUMBER() over (PARTITION BY nps.timestamp::DATE, nps.user_id ORDER BY nps.timestamp DESC) AS rownum, nps.*
          FROM {{ source('mattermost_nps', 'nps_feedback') }} nps
          {% if is_incremental() %}
          JOIN max_time mt 
          ON nps.TIMESTAMP >= mt.max_time
          {% endif %}
          WHERE nps.timestamp::DATE <= CURRENT_DATE
      )
  	where rownum = 1
    
    UNION ALL
    
        SELECT
            original_timestamp::date as date
          , useractualid as user_actual_id
          , feedback
          , id as feedback_id
        FROM (
                SELECT ROW_NUMBER() over (PARTITION BY nps.timestamp::DATE, nps.user_id ORDER BY nps.timestamp DESC) AS rownum, nps.*
                FROM {{ source('mm_plugin_prod', 'nps_nps_feedback') }} nps
                {% if is_incremental() %}
                JOIN max_time mt 
                ON nps.IMESTAMP >= mt.max_time
                {% endif %}
                WHERE nps.timestamp::TIMESTAMP <= CURRENT_TIMESTAMP
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
       daily_feedback_scores.feedback,
       daily_nps_scores.timestamp,
       daily_nps_scores.nps_id,
       daily_feedback_scores.feedback_id,
       {{ dbt_utils.surrogate_key(['daily_nps_scores.date', 'daily_nps_scores.user_actual_id', 'daily_nps_scores.user_id'])}} as id
	FROM daily_nps_scores
    LEFT JOIN daily_feedback_scores
        ON daily_nps_scores.user_actual_id = daily_feedback_scores.user_actual_id AND daily_nps_scores.date = daily_feedback_scores.date
    WHERE timestamp::date <= CURRENT_DATE)

SELECT * FROM nps_data