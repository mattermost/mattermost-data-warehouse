{{config({
    "materialized": "incremental",
    "schema": "mattermost",
    "unique_key":'id'
  })
}}


WITH max_flag_timestamp       AS (
    SELECT
        timestamp::DATE AS date
      , user_id AS server_id
      , MAX(r.timestamp)  AS max_timestamp
    FROM {{ ref('feature_flag_telemetry') }} r
    WHERE timestamp::DATE <= CURRENT_DATE
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        AND timestamp::date >= (SELECT MAX(timestamp::date) FROM {{ this }})

    {% endif %}
    GROUP BY 1, 2
),

server_feature_flag_details AS (
    SELECT 
        TIMESTAMP
      , TEST_BOOL_FEATURE
      , ORIGINAL_TIMESTAMP
      , CONTEXT_TRAITS_INSTALLATION_ID
      , CLOUD_DELINQUENT_EMAIL_JOBS_ENABLED
      , USER_ID
      , UUID_TS
      , APPS_ENABLED
      , COLLAPSED_THREADS
      , ENABLE_REMOTE_CLUSTER_SERVICE
      , ID
      , CONTEXT_IP
      , EVENT_TEXT
      , RECEIVED_AT
      , TEST_FEATURE
      , CUSTOM_USER_STATUSES
      , EVENT
      , SENT_AT
      , PLUGIN_APPS
      , ANONYMOUS_ID
      , CONTEXT_REQUEST_IP
      , PLUGIN_INCIDENT_MANAGEMENT
      , CUSTOM_DATA_RETENTION_ENABLED
      , CONTEXT_LIBRARY_NAME
      , CONTEXT_LIBRARY_VERSION
    FROM {{ ref('feature_flag_telemetry')}} ff
    JOIN max_flag_timestamp        mt
                   ON ff.user_id = mt.server_id
                       AND mt.max_timestamp = ff.timestamp
    {{dbt_utils.group_by(n=25)}}
)

SELECT *
FROM server_feature_flag_details
