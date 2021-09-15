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
    FROM {{ source('mattermost2', 'config_experimental') }}
    WHERE timestamp::DATE <= CURRENT_DATE
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        AND timestamp::date >= (SELECT MAX(date) FROM {{ this }}) - INTERVAL '1 DAY'

    {% endif %}
    GROUP BY 1, 2
),

max_rudder_timestamp       AS (
    SELECT
        timestamp::DATE AS date
      , user_id
      , MAX(r.timestamp)  AS max_timestamp
    FROM {{ source('mm_telemetry_prod', 'config_experimental') }} r
    WHERE timestamp::DATE <= CURRENT_DATE
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        AND timestamp::date >= (SELECT MAX(date) FROM {{ this }}) - INTERVAL '1 DAY'

    {% endif %}
    GROUP BY 1, 2
),

     server_experimental_details AS (
         SELECT
             COALESCE(s.timestamp::DATE, r.timestamp::date)                        AS date
           , COALESCE(s.user_id, r.user_id)                               AS server_id
           , MAX(COALESCE(s.client_side_cert_enable, r.client_side_cert_enable))            AS client_side_cert_enable
           , MAX(COALESCE(s.enable_click_to_reply, r.enable_click_to_reply))              AS enable_click_to_reply
           , MAX(COALESCE(s.enable_post_metadata, NULL))               AS enable_post_metadata
           , MAX(COALESCE(s.isdefault_client_side_cert_check, r.isdefault_client_side_cert_check))   AS isdefault_client_side_cert_check
           , MAX(COALESCE(s.link_metadata_timeout_milliseconds, r.link_metadata_timeout_milliseconds)) AS link_metadata_timeout_milliseconds
           , MAX(COALESCE(s.restrict_system_admin, r.restrict_system_admin))              AS restrict_system_admin
           , MAX(COALESCE(s.use_new_saml_library, r.use_new_saml_library))               AS use_new_saml_library           
           , {{ dbt_utils.surrogate_key(['COALESCE(s.timestamp::DATE, r.timestamp::date)', 'COALESCE(s.user_id, r.user_id)']) }} AS id
           , COALESCE(r.CONTEXT_TRAITS_INSTALLATIONID, NULL)                   AS installation_id
           , MAX(COALESCE(r.cloud_billing, NULL))               AS cloud_billing
           , MAX(COALESCE(r.enable_shared_channels, NULL))        AS enable_shared_channels
           , MAX(COALESCE(r.cloud_user_limit, NULL))        AS cloud_user_limit
           , MAX(COALESCE(r.enable_remote_cluster_service, NULL))        AS enable_remote_cluster
         FROM 
            (
              SELECT s.*
              FROM {{ source('mattermost2', 'config_experimental') }} s
              JOIN max_segment_timestamp        mt
                   ON s.user_id = mt.user_id
                       AND mt.max_timestamp = s.timestamp
            ) s
          FULL OUTER JOIN
            (
              SELECT r.*
              FROM {{ source('mm_telemetry_prod', 'config_experimental') }} r
              JOIN max_rudder_timestamp mt
                  ON r.user_id = mt.user_id
                    AND mt.max_timestamp = r.timestamp
            ) r
            ON s.timestamp::date = r.timestamp::date
            AND s.user_id = r.user_id
         GROUP BY 1, 2, 10, 11
     )
SELECT *
FROM server_experimental_details