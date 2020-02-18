{{config({
    "materialized": "incremental",
    "schema": "staging"
  })
}}

WITH max_timestamp          AS (
    SELECT
        timestamp::DATE AS date
      , user_id
      , MAX(timestamp)  AS max_timestamp
    FROM {{ source('staging_config', 'config_compliance') }}
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        WHERE timestamp::date > (SELECT MAX(date) FROM {{ this }})

    {% endif %}
    GROUP BY 1, 2
),
     server_compliance_details AS (
         SELECT
             timestamp::DATE   AS date
           , cc.user_id        AS server_id
           , MAX(enable)       AS enable_compliance
           , MAX(enable_daily) AS enable_compliance_daily
         FROM {{ source('staging_config', 'config_compliance') }} cc
              JOIN max_timestamp            mt
                   ON cc.user_id = mt.user_id
                       AND mt.max_timestamp = cc.timestamp
         GROUP BY 1, 2
     )
SELECT *
FROM server_compliance_details