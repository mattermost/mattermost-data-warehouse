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
    FROM {{ source('mattermost2', 'groups') }}
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
    FROM {{ source('mm_telemetry_prod', '_groups') }} r
    WHERE timestamp::DATE <= CURRENT_DATE
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        AND timestamp::date >= (SELECT MAX(date) FROM {{ this }}) - INTERVAL '1 DAY'

    {% endif %}
    GROUP BY 1, 2
),

     server_group_details AS (
         SELECT
             COALESCE(r.timestamp::DATE, s.timestamp::date)                        AS date
            , COALESCE(r.user_id, s.user_id)                                        AS server_id
            , MAX(COALESCE(r.UUID_TS, s.UUID_TS)) AS uuid_ts
            , MAX(COALESCE(r.RECEIVED_AT, s.RECEIVED_AT)) AS received_at
            , MAX(COALESCE(r.TIMESTAMP, s.TIMESTAMP)) AS timestamp
            , MAX(COALESCE(r.CONTEXT_IP,  NULL)) AS context_ip
            , MAX(COALESCE(r.ANONYMOUS_ID,  NULL)) AS anonymous_id
            , MAX(COALESCE(r.EVENT, s.EVENT)) AS event
            , MAX(COALESCE(r.GROUP_TEAM_COUNT, s.GROUP_TEAM_COUNT)) AS group_team_count
            , MAX(COALESCE(r.GROUP_MEMBER_COUNT, s.GROUP_MEMBER_COUNT)) AS group_member_count
            , MAX(COALESCE(r.GROUP_CHANNEL_COUNT, s.GROUP_CHANNEL_COUNT)) AS group_channel_count
            , MAX(COALESCE(r.SENT_AT, s.SENT_AT)) AS sent_at
            , MAX(COALESCE(r.DISTINCT_GROUP_MEMBER_COUNT, s.DISTINCT_GROUP_MEMBER_COUNT)) AS distinct_group_member_count
            , MAX(COALESCE(r.GROUP_SYNCED_TEAM_COUNT, s.GROUP_SYNCED_TEAM_COUNT)) AS group_synced_team_count
            , MAX(COALESCE(r.EVENT_TEXT, s.EVENT_TEXT)) AS event_text
            , MAX(COALESCE(r.ORIGINAL_TIMESTAMP, s.ORIGINAL_TIMESTAMP)) AS original_timestamp
            , MAX(COALESCE(r.GROUP_COUNT, s.GROUP_COUNT)) AS group_count
            , MAX(COALESCE(r.GROUP_SYNCED_CHANNEL_COUNT, s.GROUP_SYNCED_CHANNEL_COUNT)) AS group_synced_channel_count
            , MAX(COALESCE(r.CHANNEL,  NULL)) AS channel
            , MAX(COALESCE(r.GROUP_COUNT_WITH_ALLOW_REFERENCE, s.GROUP_COUNT_WITH_ALLOW_REFERENCE)) AS group_count_with_allow_reference
            , MAX(COALESCE(r.CONTEXT_LIBRARY_VERSION, s.CONTEXT_LIBRARY_VERSION)) AS context_library_version
            , MAX(COALESCE(r.CONTEXT_LIBRARY_NAME, s.CONTEXT_LIBRARY_NAME)) AS context_library_name
            , MAX(COALESCE(r.CONTEXT_TRAITS_INSTALLATIONID,  NULL)) AS context_traits_installationid
            , MAX(COALESCE(r.CONTEXT_REQUEST_IP,  NULL)) AS context_request_ip
            , MAX(COALESCE(r.CONTEXT_TRAITS_INSTALLATION_ID,  NULL)) AS context_traits_installation_id
            , MAX(COALESCE(r.LDAP_GROUP_COUNT,  NULL)) AS ldap_group_count
            , MAX(COALESCE(r.CUSTOM_GROUP_COUNT,  NULL)) AS custom_group_count
            , {{ dbt_utils.surrogate_key(['COALESCE(s.timestamp::DATE, r.timestamp::date)', 'COALESCE(s.user_id, r.user_id)']) }} AS id
         FROM 
            (
              SELECT s.*
              FROM {{ source('mattermost2', 'groups') }} s
              JOIN max_segment_timestamp        mt
                   ON s.user_id = mt.user_id
                       AND mt.max_timestamp = s.timestamp
            ) s
          FULL OUTER JOIN
            (
              SELECT r.*
              FROM {{ source('mm_telemetry_prod', '_groups') }} r
              JOIN max_rudder_timestamp mt
                  ON r.user_id = mt.user_id
                    AND mt.max_timestamp = r.timestamp
            ) r
            ON s.timestamp::date = r.timestamp::date
            AND s.user_id = r.user_id
         GROUP BY 1, 2, 28
     )

     SELECT *
     FROM server_group_details