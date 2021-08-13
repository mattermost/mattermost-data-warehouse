{{config({
    "materialized": "incremental",
    "schema": "staging",
    "unique_key":'id'
  })
}}

WITH max_segment_timestamp            AS (
    SELECT
        timestamp::DATE AS date
      , user_id
      , MAX(timestamp)  AS max_timestamp
    FROM {{ source('mattermost2', 'channel_moderation') }}
    WHERE timestamp::DATE <= CURRENT_DATE
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        AND timestamp::date >= (SELECT MAX(date) FROM {{ this }}) - INTERVAL '1 DAY'

    {% endif %}
    GROUP BY 1, 2
),

max_rudder_timestamp            AS (
    SELECT
        timestamp::DATE AS date
      , user_id
      , MAX(timestamp)  AS max_timestamp
    FROM {{ source('mm_telemetry_prod', 'channel_moderation') }}
    WHERE timestamp::DATE <= CURRENT_DATE
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        AND timestamp::date >= (SELECT MAX(date) FROM {{ this }}) - INTERVAL '1 DAY'

    {% endif %}
    GROUP BY 1, 2
),

     server_channel_moderation_details AS (
         SELECT
             COALESCE(r.timestamp::DATE, s.timestamp::date)                        AS date
           , COALESCE(r.user_id, s.user_id)                                        AS server_id
           , MAX(COALESCE(r.channel_scheme_count, s.channel_scheme_count))         AS channel_scheme_count
           , MAX(COALESCE(r.create_post_guest_disabled_count, s.create_post_guest_disabled_count))     AS create_post_guest_disabled_count
           , MAX(COALESCE(r.create_post_user_disabled_count, s.create_post_user_disabled_count))  AS create_post_user_disabled_count
           , MAX(COALESCE(r.manage_members_user_disabled_count, s.manage_members_user_disabled_count)) AS manage_members_user_disabled_count
           , MAX(COALESCE(r.post_reactions_guest_disabled_count, s.post_reactions_guest_disabled_count)) AS post_reactions_guest_disabled_count
           , MAX(COALESCE(r.post_reactions_user_disabled_count, s.post_reactions_user_disabled_count)) AS post_reactions_user_disabled_count
           , MAX(COALESCE(r.use_channel_mentions_guest_disabled_count, s.use_channel_mentions_guest_disabled_count)) AS use_channel_mentions_guest_disabled_count
           , MAX(COALESCE(r.use_channel_mentions_user_disabled_count, s.use_channel_mentions_user_disabled_count)) AS use_channel_mentions_user_disabled_count
           , {{ dbt_utils.surrogate_key(['COALESCE(r.timestamp::DATE, s.timestamp::date)', 'COALESCE(r.user_id, s.user_id)']) }} AS id
           , COALESCE(r.CONTEXT_TRAITS_INSTALLATIONID, NULL)                       AS installation_id
         FROM 
            (
              SELECT s.*
              FROM {{ source('mattermost2', 'channel_moderation') }} s
              JOIN max_segment_timestamp        mt
                   ON s.user_id = mt.user_id
                       AND mt.max_timestamp = s.timestamp
            ) s
          FULL OUTER JOIN
            (
              SELECT r.*
              FROM {{ source('mm_telemetry_prod', 'channel_moderation') }} r
              JOIN max_rudder_timestamp mt
                  ON r.user_id = mt.user_id
                    AND mt.max_timestamp = r.timestamp
            ) r
            ON s.timestamp::date = r.timestamp::date
            AND s.user_id = r.user_id
         GROUP BY 1, 2, 11, 12
     )
SELECT *
FROM server_channel_moderation_details