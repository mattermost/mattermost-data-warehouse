{{config({
    "materialized": 'incremental',
    "schema": "mattermost",
    "unique_key": "id"
  })
}}

WITH upgrade         AS (
    SELECT
        date
      , server_id
      , max(account_sfid) OVER (PARTITION BY server_id)          AS account_sfid
      , license_id1                                              AS license_id
      , lag(version) OVER (PARTITION BY server_id ORDER BY date) AS prev_version
      , version                                                  AS current_version
      , edition                                                  AS current_edition
      , lag(edition) OVER (PARTITION BY server_id ORDER BY date) AS prev_edition
    FROM {{ ref('server_daily_details') }}
    WHERE NOT tracking_disabled
                        ),
     server_upgrades AS (
         SELECT
             date
           , server_id
           , account_sfid
           , license_id
           , NULLIF(CASE WHEN regexp_substr(regexp_substr(prev_version,'^[0-9]{0,}[.]{1}[0-9[{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}'), '[0-9]{0,}[.]{1}[0-9[{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}$') IS NULL THEN
                    regexp_substr(prev_version, '^[0-9]{0,}[.]{1}[0-9[{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}') 
                  ELSE regexp_substr(regexp_substr(prev_version,'^[0-9]{0,}[.]{1}[0-9[{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}'), '[0-9]{0,}[.]{1}[0-9[{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}$') 
                END, '') AS prev_version
           , NULLIF(CASE WHEN regexp_substr(regexp_substr(current_version,'^[0-9]{0,}[.]{1}[0-9[{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}'), '[0-9]{0,}[.]{1}[0-9[{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}$') IS NULL THEN
                    regexp_substr(current_version, '^[0-9]{0,}[.]{1}[0-9[{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}') 
                  ELSE regexp_substr(regexp_substr(current_version,'^[0-9]{0,}[.]{1}[0-9[{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}'), '[0-9]{0,}[.]{1}[0-9[{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}$') 
                END, '')  AS current_version
           , NULLIF(prev_edition, '') AS prev_edition
           , NULLIF(current_edition, '') AS current_edition
           , {{ dbt_utils.surrogate_key(['date', 'server_id']) }} as id
         FROM upgrade
         WHERE (
              (CASE WHEN NULLIF(SPLIT_PART(current_version, '.', 1),'')::FLOAT > NULLIF(SPLIT_PART(COALESCE(prev_version, current_version), '.', 1),'')::float THEN TRUE
                WHEN NULLIF(SPLIT_PART(current_version, '.', 1),'')::FLOAT = NULLIF(SPLIT_PART(COALESCE(prev_version, current_version), '.', 1),'')::float
                  AND NULLIF(SPLIT_PART(current_version, '.', 2),'')::FLOAT > NULLIF(SPLIT_PART(COALESCE(prev_version, current_version), '.', 2),'')::float THEN TRUE
                
                WHEN NULLIF(SPLIT_PART(current_version, '.', 1),'')::FLOAT = NULLIF(SPLIT_PART(COALESCE(prev_version, current_version), '.', 1), '')::float
                  
                  AND NULLIF(SPLIT_PART(CASE WHEN regexp_substr(regexp_substr(current_version,'^[0-9]{0,}[.]{1}[0-9[{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}'), '[0-9]{0,}[.]{1}[0-9[{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}$') IS NULL THEN
                                  regexp_substr(current_version, '^[0-9]{0,}[.]{1}[0-9[{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}') 
                                  ELSE regexp_substr(regexp_substr(current_version,'^[0-9]{0,}[.]{1}[0-9[{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}'), '[0-9]{0,}[.]{1}[0-9[{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}$') 
                                END, '.', 2),'')::FLOAT = NULLIF(SPLIT_PART(COALESCE(
                                                        CASE WHEN regexp_substr(regexp_substr(prev_version,'^[0-9]{0,}[.]{1}[0-9[{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}'), '[0-9]{0,}[.]{1}[0-9[{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}$') IS NULL THEN
                                                            regexp_substr(prev_version, '^[0-9]{0,}[.]{1}[0-9[{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}') 
                                                        ELSE regexp_substr(regexp_substr(prev_version,'^[0-9]{0,}[.]{1}[0-9[{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}'), '[0-9]{0,}[.]{1}[0-9[{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}$') 
                                                        END
                                                        ,CASE WHEN regexp_substr(regexp_substr(current_version,'^[0-9]{0,}[.]{1}[0-9[{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}'), '[0-9]{0,}[.]{1}[0-9[{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}$') IS NULL THEN
                                                              regexp_substr(current_version, '^[0-9]{0,}[.]{1}[0-9[{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}') 
                                                            ELSE regexp_substr(regexp_substr(current_version,'^[0-9]{0,}[.]{1}[0-9[{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}'), '[0-9]{0,}[.]{1}[0-9[{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}$') 
                                                          END), '.', 2),'')::float
                  
                  AND NULLIF(SPLIT_PART(CASE WHEN regexp_substr(regexp_substr(current_version,'^[0-9]{0,}[.]{1}[0-9[{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}'), '[0-9]{0,}[.]{1}[0-9[{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}$') IS NULL THEN
                                  regexp_substr(current_version, '^[0-9]{0,}[.]{1}[0-9[{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}') 
                                  ELSE regexp_substr(regexp_substr(current_version,'^[0-9]{0,}[.]{1}[0-9[{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}'), '[0-9]{0,}[.]{1}[0-9[{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}$') 
                                END, '.', 3),'')::FLOAT > 
                                            NULLIF(SPLIT_PART(COALESCE(
                                                        CASE WHEN regexp_substr(regexp_substr(prev_version,'^[0-9]{0,}[.]{1}[0-9[{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}'), '[0-9]{0,}[.]{1}[0-9[{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}$') IS NULL THEN
                                                            regexp_substr(prev_version, '^[0-9]{0,}[.]{1}[0-9[{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}') 
                                                        ELSE regexp_substr(regexp_substr(prev_version,'^[0-9]{0,}[.]{1}[0-9[{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}'), '[0-9]{0,}[.]{1}[0-9[{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}$') 
                                                        END
                                                        , CASE WHEN regexp_substr(regexp_substr(current_version,'^[0-9]{0,}[.]{1}[0-9[{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}'), '[0-9]{0,}[.]{1}[0-9[{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}$') IS NULL THEN
                                                              regexp_substr(current_version, '^[0-9]{0,}[.]{1}[0-9[{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}') 
                                                            ELSE regexp_substr(regexp_substr(current_version,'^[0-9]{0,}[.]{1}[0-9[{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}'), '[0-9]{0,}[.]{1}[0-9[{0,}[.]{1}[0-9]{0,}[.]{1}[0-9]{0,}$') 
                                                          END), '.', 3), '')::float THEN TRUE
                ELSE FALSE END)
             OR (NULLIF(current_edition, '') = 'true' AND coalesce(prev_edition, 'true') = 'false')
             )
         AND date <= CURRENT_DATE
         {% if is_incremental() %}

         AND date >= (SELECT MAX(date) FROM {{this}} )

         {% endif %}
     )
SELECT *
FROM server_upgrades
