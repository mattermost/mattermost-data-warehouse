{{config({
    "materialized": "incremental",
    "schema": "staging",
    "incremental_strategy": "delete+insert",
    "unique_key":'id',
    "tags":'hourly'
  })
}}

WITH rudder_servers AS (
  SELECT * 
  FROM {{ source('mm_telemetry_prod', 'server') }}
  WHERE original_timestamp::date <= CURRENT_DATE
  {% if is_incremental() %}
  
  AND original_timestamp::DATE >= (SELECT MAX(DATE) - INTERVAL '2 DAY' FROM {{this}})

  {% endif %}
),

segment_servers AS (
  SELECT * 
  FROM {{ source('mattermost2', 'server') }}
  WHERE timestamp::date <= CURRENT_DATE
  {% if is_incremental() %}
  
  AND timestamp::date >= (SELECT MAX(DATE) - INTERVAL '1 DAY' FROM {{this}})

  {% endif %}
),

server_details AS (
  SELECT
    COALESCE(s2.anonymous_id, s1.user_id)                            AS annonymous_id
  , COALESCE(s2.channel, ''::VARCHAR)                                AS channel
  , COALESCE(s2.context_ip, s2.context_request_ip, ''::VARCHAR)                             AS context_ip
  , COALESCE(s2.context_library_name, s1.context_library_name)       AS context_library_name
  , COALESCE(s2.context_library_version, s1.context_library_version) AS context_library_version
  , COALESCE(s2.database_type, s1.database_type)                     AS database_type
  , COALESCE(s2.database_version, s1.database_version)               AS database_version
  , COALESCE(s2.edition, s1.edition)                                 AS edition
  , COALESCE(s2.event, s1.event)                                     AS event
  , COALESCE(s2.event_text, s1.event_text)                           AS event_text
  , COALESCE(s2.id, s1.id)                                           AS id
  , COALESCE(s2.operating_system, s1.operating_system)               AS operating_system
  , COALESCE(s2.system_admins, s1.system_admins)                     AS system_admins
  , MAX(COALESCE(s2.original_timestamp, s1.timestamp))               AS timestamp
  , COALESCE(s2.user_id, s1.user_id)                                 AS user_id
  , MAX(COALESCE(s2.uuid_ts, s1.uuid_ts))                            AS uuid_ts
  , COALESCE(s2.version, s1.version)                                 AS version
  , MAX(COALESCE(s2.sent_at, s1.sent_at))                            AS sent_at
  , MAX(COALESCE(s2.received_at, s1.received_at))                    AS received_at
  , COALESCE(s2.CONTEXT_TRAITS_INSTALLATIONID, 
             s2.context_traits_installation_id)                      AS installation_id
  , COALESCE(s2.installation_type, NULL)                             AS installation_type
FROM segment_servers                       s1
     FULL OUTER JOIN rudder_servers s2
                     ON s1.user_id = s2.user_id
                         AND s1.timestamp::DATE = s2.original_timestamp::DATE
WHERE COALESCE(s2.original_timestamp::date, s1.timestamp::date) <= CURRENT_DATE
{% if is_incremental() %}

AND COALESCE(s2.original_timestamp, s1.timestamp)::date >= (SELECT MAX(date) - interval '2 day' FROM {{this}})

{% endif %}
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 15, 17, 20, 21
),
max_timestamp              AS (
    SELECT
        s1.timestamp::DATE         AS date
      , s1.user_id                 AS user_id
      , MAX(s1.timestamp)          AS max_timestamp
      , COUNT(s1.user_id)          AS occurrences
    FROM server_details s1
    WHERE s1.timestamp::DATE <= CURRENT_DATE
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        AND s1.timestamp >= (SELECT MAX(timestamp) FROM {{ this }}) - interval '48 hours'

    {% endif %}
    GROUP BY 1, 2
),
     license                 AS (
         SELECT
             l.license_id
           , l.server_id
           , MAX(l.customer_id) AS customer_id
           , MAX(l.company) AS company 
           , MAX(l.edition) AS edition
           , MAX(l.issued_date) AS issued_date
           , MAX(l.start_date) AS start_date
           , MAX(l.server_expire_date_join)   AS expire_date
           , MAX(l.master_account_sfid) AS master_account_sfid
           , MAX(l.master_account_name) AS master_account_name
           , MAX(l.account_sfid) AS account_sfid
           , MAX(l.account_name) AS account_name
           , MAX(l.license_email) AS license_email
           , MAX(l.contact_sfid) AS contact_sfid
           , MAX(l.contact_email) AS contact_email
           , MAX(l.number) AS number
           , MAX(l.stripeid) AS stripeid
           , MAX(l.users) AS users
           , MAX(l.has_trial_and_non_trial) AS has_trial_and_non_trial 
           , MAX(l.trial) AS trial
         FROM {{ ref('licenses') }} l
         GROUP BY 1, 2
     ),
     server_server_details AS (
         SELECT
             s.timestamp::DATE                    AS date
           , s.user_id                            AS server_id
           , MAX(s.version)                       AS version
           , MAX(s.context_library_version)       AS context_library_version
           , s.edition
           , MAX(s.system_admins)                 AS system_admins
           , s.operating_system
           , s.database_type
           , s.event
           , s.event_text
           , MAX(s.sent_at)                       AS sent_at
           , MAX(s.received_at)                   AS received_at
           , s.timestamp
           , s.timestamp                          AS original_timestamp
           , mt.occurrences
           , MAX(license.master_account_sfid)     AS master_account_sfid
           , MAX(license.account_sfid)            AS account_sfid
           , MAX(CASE WHEN license.has_trial_and_non_trial AND NOT license.trial THEN license.license_id
                 WHEN NOT license.has_trial_and_non_trial THEN license.license_id
                 ELSE NULL END)              AS license_id1
           , MAX(CASE WHEN license.has_trial_and_non_trial AND license.trial THEN license.license_id
                 ELSE NULL END)              AS license_id2
           , MAX(license.license_email)           AS license_email
           , MAX(license.contact_sfid)            AS license_contact_sfid
           , {{ dbt_utils.surrogate_key(['s.timestamp::date', 's.user_id']) }} AS id
           , s.context_ip                         AS context_ip
           , MAX(s.database_version)              AS database_version
           , s.installation_id                    AS installation_id
           , s.installation_type                  AS installation_type
           , MAX(s.uuid_ts)                       AS uuid_ts
         FROM server_details s
              JOIN max_timestamp mt
                   ON s.user_id = mt.user_id
                       AND s.timestamp = mt.max_timestamp
              LEFT JOIN license
                        ON s.user_id = license.server_id
                            AND s.timestamp::date >= license.issued_date
                            AND s.timestamp::date <= license.expire_date
        {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        WHERE s.timestamp::date >= (SELECT MAX(DATE) - interval '2 day' FROM {{ this }})

         {% endif %}
         GROUP BY 1, 2, 5, 7, 8, 9, 10, 13, 14, 15, 22, 23, 25, 26
     )
SELECT *
FROM server_server_details