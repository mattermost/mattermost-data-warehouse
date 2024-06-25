{{config({
    "materialized": 'incremental',
    "unique_key": 'id',
    "schema": "mattermost",
    "tags":["nightly"]
  })
}}
{% if is_incremental() %}
  WITH max_date AS (
    SELECT MAX(DATE) AS max_date
    FROM {{this}}
  ),

 nps_data AS (
{% else %}
WITH nps_data                       AS (
{% endif %} 
    SELECT
        last_score_date
      , server_id
      , user_id
      , server_version
      , score
      , license_id
      , license_sku
      , promoter_type
      , user_created_at
      , server_install_date
      , responses_alltime
      , user_role
      , MAX(feedback)               AS feedback
      , MAX(last_feedback_date)     AS last_feedback_date
      , MAX(date)                   AS last_date
    FROM {{ ref('nps_user_daily_score') }}
    {{ dbt_utils.group_by(n=12) }}
                                       ),

     min_nps_by_version             AS (
         SELECT
             server_id
           , user_id
           , server_version
           , last_score_date      AS min_version_nps_date
           , CASE WHEN MAX(last_date) OVER (PARTITION BY server_id, user_id, server_version) = last_date
                THEN CURRENT_DATE ELSE last_date END       AS last_date
         FROM nps_data
     ),

     nps_server_vesion              AS (
         SELECT
             d.date
           , nps.server_id
           , nps.user_id
           , nps.server_version
           , nps.last_date
           , min_version_nps_date
           , {{ dbt_utils.surrogate_key(['d.date', 'nps.user_id', 'nps.server_id', 'nps.server_version']) }} AS id
         FROM {{ ref('dates') }}            d
              JOIN min_nps_by_version nps
                   ON d.date >= nps.min_version_nps_date
                       AND d.date <= nps.last_date
         {{ dbt_utils.group_by(n=7) }}
     ),
     nps_server_version_daily_score AS (
         SELECT
             n1.date
           , n2.server_id
           , n2.user_id
           , n2.user_role
           , n2.last_score_date
           , n2.server_version
           , n2.score
           , n2.license_id
           , n2.license_sku
           , n2.promoter_type
           , n2.user_created_at
           , n2.server_install_date
           , n2.responses_alltime
           , n2.feedback
           , n2.last_feedback_date
           , n1.id
         FROM nps_server_vesion n1
              JOIN nps_data     n2
                   ON n1.server_id = n2.server_id
                       AND n1.user_id = n2.user_id
                       AND n1.server_version = n2.server_version
                       AND n1.date <= n2.last_date
                       AND n1.date >= n2.last_score_date
           {% if is_incremental() %}
              JOIN max_date
                   ON n1.date >= max_date.max_date
           {% endif %}
        {{ dbt_utils.group_by(n=16) }}
     )
     SELECT *
     FROM nps_server_version_daily_score