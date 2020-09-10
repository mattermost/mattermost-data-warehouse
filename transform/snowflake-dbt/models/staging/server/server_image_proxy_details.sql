{{config({
    "materialized": "incremental",
    "schema": "staging",
    "unique_key":'id'
  })
}}

WITH max_segment_timestamp                AS (
    SELECT
        timestamp::DATE AS date
      , user_id
      , MAX(timestamp)  AS max_timestamp
    FROM {{ source('mattermost2', 'config_image_proxy') }}
    WHERE timestamp::DATE <= CURRENT_DATE
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        AND timestamp::date >= (SELECT MAX(date) FROM {{ this }})

    {% endif %}
    GROUP BY 1, 2
),

max_rudder_timestamp                AS (
    SELECT
        timestamp::DATE AS date
      , user_id
      , MAX(timestamp)  AS max_timestamp
    FROM {{ source('mattermost2', 'config_image_proxy') }}
    WHERE timestamp::DATE <= CURRENT_DATE
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        AND timestamp::date >= (SELECT MAX(date) FROM {{ this }})

    {% endif %}
    GROUP BY 1, 2
),
     server_image_proxy_details AS (
         SELECT
             COALESCE(s.timestamp::DATE, r.timestamp::date)              AS date
           , COALESCE(s.user_id, r.user_id)                              AS server_id
           , MAX(COALESCE(s.enable, r.enable))                               AS enable_image_proxy
           , MAX(COALESCE(s.image_proxy_type, r.image_proxy_type))                     AS image_proxy_type
           , MAX(COALESCE(s.isdefault_remote_image_proxy_options, r.isdefault_remote_image_proxy_options)) AS isdefault_remote_image_proxy_options
           , MAX(COALESCE(s.isdefault_remote_image_proxy_url, r.isdefault_remote_image_proxy_url))     AS isdefault_remote_image_proxy_url           
           , {{ dbt_utils.surrogate_key('COALESCE(s.timestamp::DATE, r.timestamp::date)', 'COALESCE(s.user_id, r.user_id)') }} AS id
           , COALESCE(r.CONTEXT_TRAITS_INSTALLATIONID, NULL)                   AS installation_id
           FROM 
            (
              SELECT s.*
              FROM {{ source('mattermost2', 'config_image_proxy') }} s
              JOIN max_segment_timestamp        mt
                   ON s.user_id = mt.user_id
                       AND mt.max_timestamp = s.timestamp
            ) s
          FULL OUTER JOIN
            (
              SELECT r.*
              FROM {{ source('mm_telemetry_prod', 'config_image_proxy') }} r
              JOIN max_rudder_timestamp mt
                  ON r.user_id = mt.user_id
                    AND mt.max_timestamp = r.timestamp
            ) r
            ON s.timestamp::date = r.timestamp::date
            AND s.user_id = r.user_id
         GROUP BY 1, 2, 7, 8
     )
SELECT *
FROM server_image_proxy_details