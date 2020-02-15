{{config({
    "materialized": "incremental",
    "schema": "staging"
  })
}}

WITH max_timestamp                AS (
    SELECT
        timestamp::DATE AS date
      , user_id
      , max(timestamp)  AS max_timestamp
    FROM {{ source('staging_config', 'config_guest_accounts') }}
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        WHERE timestamp::date > (SELECT MAX(date) FROM {{ this }})

    {% endif %}
    GROUP BY 1, 2
),
     server_guest_account_details AS (
         SELECT
             timestamp::DATE                             AS date
           , g.user_id                                   AS server_id
           , max(allow_email_accounts)                   AS allow_email_accounts
           , max(enable)                                 AS enable_guest_accounts
           , max(enforce_multifactor_authentication)     AS enforce_guest_multifactor_auth
           , max(isdefault_restrict_creation_to_domains) AS isdefault_restrict_creation_to_domains
         FROM {{ source('staging_config', 'config_guest_accounts') }} g
              JOIN max_timestamp                mt
                   ON g.user_id = mt.user_id
                       AND mt.max_timestamp = g.timestamp
         GROUP BY 1, 2
     )
SELECT *
FROM server_guest_account_details