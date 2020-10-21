{{config({
    "materialized": "incremental",
    "schema": "blapi",
    "unique_key":"id",
    "alias":"subscriptions",
    "tags":["hourly","blapi"]
  })
}}

WITH runrate AS (
    SELECT 
        u1.subscription_id
      , u2.max_date
      , MAX(u1.active_users) AS max_users_previous_day
    FROM {{ source('blapi', 'usage_events') }} u1
    JOIN 
    (
        SELECT 
            subscription_id
        , CASE WHEN max(timestamp::date) = CURRENT_DATE THEN MAX(timestamp::date) - INTERVAL '1 DAY'
            WHEN MAX(timestamp::date) = CURRENT_DATE - INTERVAL '1 DAY' THEN MAX(TIMESTAMP::DATE) 
            WHEN MAX(timestamp::date) > CURRENT_DATE THEN MAX(CURRENT_DATE - INTERVAL '1 DAY') 
            ELSE NULL END AS max_date
        FROM {{ source('blapi', 'usage_events') }}
        GROUP BY 1
    ) u2
        ON u1.subscription_id = u2.subscription_id
        AND u1.timestamp::date = u2.max_date
    GROUP BY 1, 2
),

subscriptions AS (
    SELECT 
        s.*
      , (rr.max_users_previous_day * 10) AS runrate
    FROM {{ source('blapi', 'subscriptions') }} s
    LEFT JOIN runrate rr
        ON s.id = rr.subscription_id
)

SELECT 
    *
FROM subscriptions