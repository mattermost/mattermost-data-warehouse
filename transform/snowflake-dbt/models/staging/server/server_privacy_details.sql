{{config({
    "materialized": "incremental",
    "schema": "staging",
    "unique_key":'id'
  })
}}

WITH max_segment_timestamp        AS (
    SELECT
        timestamp::DATE AS date
      , user_id
      , MAX(timestamp)  AS max_timestamp
    FROM {{ source('mattermost2', 'config_privacy') }}
    WHERE timestamp::DATE <= CURRENT_DATE
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        AND timestamp::date >= (SELECT MAX(date) FROM {{ this }})

    {% endif %}
    GROUP BY 1, 2
),

max_rudder_timestamp       AS (
    SELECT
        timestamp::DATE AS date
      , user_id
      , MAX(r.timestamp)  AS max_timestamp
    FROM {{ source('mm_telemetry_prod', 'config_privacy') }} r
    WHERE timestamp::DATE <= CURRENT_DATE
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        AND timestamp::date >= (SELECT MAX(date) FROM {{ this }})

    {% endif %}
    GROUP BY 1, 2
),
     server_privacy_details AS (
         SELECT
             COALESCE(r.timestamp::DATE, s.timestamp::date)                        AS date
           , COALESCE(r.user_id, s.user_id)                                        AS server_id
           , MAX(COALESCE(r.show_email_address, s.show_email_address)) AS show_email_address
           , MAX(COALESCE(r.show_full_name, s.show_full_name))     AS show_full_name           
           , {{ dbt_utils.surrogate_key('COALESCE(s.timestamp::DATE, r.timestamp::date)', 'COALESCE(s.user_id, r.user_id)', 'COALESCE(r.CONTEXT_TRAITS_INSTALLATIONID, NULL)') }} AS id
           , COALESCE(r.CONTEXT_TRAITS_INSTALLATIONID, NULL)                   AS installation_id
         FROM 
            (
              SELECT s.*
              FROM {{ source('mattermost2', 'config_privacy') }} s
              JOIN max_segment_timestamp        mt
                   ON s.user_id = mt.user_id
                       AND mt.max_timestamp = s.timestamp
            ) s
          FULL OUTER JOIN
            (
              SELECT r.*
              FROM {{ source('mm_telemetry_prod', 'config_privacy') }} r
              JOIN max_rudder_timestamp mt
                  ON r.user_id = mt.user_id
                    AND mt.max_timestamp = r.timestamp
            ) r
            ON s.timestamp::date = r.timestamp::date
            AND s.user_id = r.user_id
         GROUP BY 1, 2, 5, 6
     )
SELECT *
FROM server_privacy_details