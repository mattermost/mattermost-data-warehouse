{{config({
    "materialized": 'incremental',
    "schema": "blp",
    "unique_key": 'id',
    "tags":'hourly'
  })
}}

{% if is_incremental() %}

{{ license_cleaning('license_server_fact') }}

{% endif %}

with license_union as (
  SELECT *
  FROM {{ ref('licensed_servers') }}
  
  UNION ALL

  SELECT *
  FROM {{ ref('nonactivated_licenses') }}
),

last_server_telemetry as (
  SELECT 
        server_id
      , MAX(date) as max_date
  FROM {{ ref('server_activity_details') }}
  WHERE DATE <= CURRENT_DATE
  GROUP BY 1
),

server_activity AS (
  SELECT a1.*
  FROM {{ ref('server_activity_details') }} a1
  JOIN last_server_telemetry a2
       ON a1.server_id = a2.server_id
       AND a1.date = a2.max_date
),
license_server_fact as (
SELECT 
        id
      , server_id
      , license_id
      , COALESCE(
                COALESCE(account_sfid
                      , FIRST_VALUE(account_sfid IGNORE NULLS) OVER (PARTITION BY COALESCE(server_id
                                                                    , license_id) ORDER BY last_active_date desc)
                      , MAX(account_sfid) OVER (PARTITION BY COALESCE(contact_sfid
                                                                    , license_id))
                      , MAX(account_sfid) OVER (PARTITION BY COALESCE(CASE WHEN SPLIT_PART(lower(license_email), '@', 2) NOT IN 
                                                                          (SELECT DOMAIN_NAME FROM {{ source('util', 'public_domains')}} GROUP BY 1) 
                                                                          THEN SPLIT_PART(lower(license_email), '@', 2)
                                                                        ELSE contact_sfid END
                                                                      , license_id))
                      , MAX(account_sfid) OVER (PARTITION BY COALESCE(customer_id
                                                                    , license_id)))
              , customer_id
      )                                                                               AS customer_id
      , COALESCE(
                 COALESCE(account_name
                        , FIRST_VALUE(account_name IGNORE NULLS) OVER (PARTITION BY COALESCE(server_id
                                                                      , license_id) ORDER BY last_active_date desc)
                        , MAX(account_name) OVER (PARTITION BY COALESCE(contact_sfid
                                                                      , license_id))
                        , MAX(account_name) OVER (PARTITION BY COALESCE(CASE WHEN SPLIT_PART(lower(license_email), '@', 2) NOT IN 
                                                                          (SELECT DOMAIN_NAME FROM {{ source('util', 'public_domains')}} GROUP BY 1) 
                                                                            THEN SPLIT_PART(lower(license_email), '@', 2)
                                                                          ELSE contact_sfid END
                                                                        , license_id))
                       , MAX(account_name) OVER (PARTITION BY COALESCE(customer_id
                                                                      , license_id)))
                , COALESCE(company
                        , FIRST_VALUE(company IGNORE NULLS) OVER (PARTITION BY COALESCE(server_id
                                                                  , license_id) ORDER BY last_active_date desc)
                        , MAX(company) OVER (PARTITION BY COALESCE(contact_sfid
                                                                  , license_id))
                        , MAX(company) OVER (PARTITION BY COALESCE(CASE WHEN SPLIT_PART(lower(license_email), '@', 2) NOT IN 
                                                                          (SELECT DOMAIN_NAME FROM {{ source('util', 'public_domains')}} GROUP BY 1) 
                                                                      THEN SPLIT_PART(lower(license_email), '@', 2)
                                                                    ELSE contact_sfid END
                                                                  , license_id))
                        , MAX(company) OVER (PARTITION BY COALESCE(customer_id
                                                                  , license_id))
                        , MAX(company) OVER (PARTITION BY COALESCE(account_sfid
                                                                  , license_id)))
      )                                                                                     AS customer_name
      , COALESCE(
                 COALESCE(account_name
                        , FIRST_VALUE(account_name IGNORE NULLS) OVER (PARTITION BY COALESCE(server_id
                                                                      , license_id) ORDER BY last_active_date desc)
                        , MAX(account_name) OVER (PARTITION BY COALESCE(contact_sfid
                                                                      , license_id))
                        , MAX(account_name) OVER (PARTITION BY COALESCE(CASE WHEN SPLIT_PART(lower(license_email), '@', 2) NOT IN 
                                                                          (SELECT DOMAIN_NAME FROM {{ source('util', 'public_domains')}} GROUP BY 1) 
                                                                            THEN SPLIT_PART(lower(license_email), '@', 2)
                                                                          ELSE contact_sfid END
                                                                        , license_id))
                        , MAX(account_name) OVER (PARTITION BY COALESCE(customer_id
                                                                      , license_id))),
                  COALESCE(company
                , FIRST_VALUE(company IGNORE NULLS) OVER (PARTITION BY COALESCE(server_id
                                                          , license_id) ORDER BY last_active_date desc)
                , MAX(company) OVER (PARTITION BY COALESCE(account_sfid
                                                          , license_id))
                , MAX(company) OVER (PARTITION BY COALESCE(contact_sfid
                                                          , license_id))
                , MAX(company) OVER (PARTITION BY COALESCE(CASE WHEN SPLIT_PART(lower(license_email), '@', 2) NOT IN 
                                                                          (SELECT DOMAIN_NAME FROM {{ source('util', 'public_domains')}} GROUP BY 1) 
                                                              THEN SPLIT_PART(lower(license_email), '@', 2)
                                                            ELSE contact_sfid END
                                                          , license_id))
                , MAX(company) OVER (PARTITION BY COALESCE(customer_id
                                                          , license_id)))) as company
      , edition
      , users
      , trial
      , issued_date
      , start_date
      , expire_date
      , license_email
      , COALESCE(contact_sfid
                , MAX(contact_sfid) OVER (PARTITION BY license_email)) as contact_sfid
      , COALESCE(account_sfid
                , FIRST_VALUE(account_sfid IGNORE NULLS) OVER (PARTITION BY COALESCE(server_id
                                                               , license_id) ORDER BY last_active_date desc)
                , MAX(account_sfid) OVER (PARTITION BY COALESCE(customer_id
                                                               , license_id))
                , MAX(account_sfid) OVER (PARTITION BY COALESCE(contact_sfid
                                                               , license_id))
                , MAX(account_sfid) OVER (PARTITION BY COALESCE(CASE WHEN SPLIT_PART(lower(license_email), '@', 2) NOT IN 
                                                                          (SELECT DOMAIN_NAME FROM {{ source('util', 'public_domains')}} GROUP BY 1) 
                                                                    THEN SPLIT_PART(lower(license_email), '@', 2)
                                                                  ELSE contact_sfid END
                                                                , license_id))) AS account_sfid
    , COALESCE(account_name
                , FIRST_VALUE(account_name IGNORE NULLS) OVER (PARTITION BY COALESCE(server_id
                                                               , license_id)  ORDER BY last_active_date desc)
                , MAX(account_name) OVER (PARTITION BY COALESCE(customer_id
                                                               , license_id))
                , MAX(account_name) OVER (PARTITION BY COALESCE(contact_sfid
                                                               , license_id))
                , MAX(account_name) OVER (PARTITION BY COALESCE(CASE WHEN SPLIT_PART(lower(license_email), '@', 2) NOT IN 
                                                                          (SELECT DOMAIN_NAME FROM {{ source('util', 'public_domains')}} GROUP BY 1) 
                                                                    THEN SPLIT_PART(lower(license_email), '@', 2)
                                                                  ELSE contact_sfid END
                                                                , license_id))) AS account_name
      , opportunity_sfid
      , stripeid
      , customer_id   AS license_customer_id
      , license_activation_date::TIMESTAMP AS license_activation_date
      , last_active_date
      , server_activation_date
      , ROW_NUMBER() OVER (PARTITION BY license_id ORDER BY LAST_ACTIVE_DATE NULLS LAST) AS LICENSE_RANK
       , ROW_NUMBER() OVER (PARTITION BY coalesce(server_id, license_id) ORDER BY last_active_date desc NULLS LAST) AS license_priority_rank
       , CASE WHEN COALESCE((lead(start_date, 4) OVER (PARTITION BY COALESCE(server_id, license_id) ORDER BY start_date, issued_date))::DATE, expire_date::DATE + INTERVAL '1 DAY') <= expire_date 
            THEN (lead(start_date, 4) OVER (PARTITION BY COALESCE(server_id, license_id) ORDER BY start_date, issued_date))::DATE - interval '1 day'
          WHEN COALESCE((lead(start_date, 3) OVER (PARTITION BY COALESCE(server_id, license_id) ORDER BY start_date, issued_date))::DATE, expire_date::DATE + INTERVAL '1 DAY') <= expire_date 
            THEN (lead(start_date, 3) OVER (PARTITION BY COALESCE(server_id, license_id) ORDER BY start_date, issued_date))::DATE - interval '1 day'
          WHEN COALESCE((lead(start_date, 2) OVER (PARTITION BY COALESCE(server_id, license_id) ORDER BY start_date, issued_date))::DATE, expire_date::DATE + INTERVAL '1 DAY') <= expire_date 
            THEN (lead(start_date, 2) OVER (PARTITION BY COALESCE(server_id, license_id) ORDER BY start_date, issued_date))::DATE - interval '1 day'
          WHEN COALESCE((lead(start_date, 1) OVER (PARTITION BY COALESCE(server_id, license_id) ORDER BY start_date, issued_date))::DATE, expire_date::DATE + INTERVAL '1 DAY') <= expire_date
            THEN (lead(start_date, 1) OVER (PARTITION BY COALESCE(server_id, license_id) ORDER BY start_date, issued_date))::DATE - interval '1 day'
          ELSE expire_date END as license_retired_date
FROM license_union

UNION ALL

SELECT *
FROM {{ ref('cloud_subscriptions_blp') }}
WHERE license_id NOT IN (select license_id from license_union group by 1)
)

SELECT 
     l.id                      
   , l.server_id               
   , l.license_id              
   , l.customer_id             
   , l.customer_name           
   , l.company                 
   , CASE WHEN l.edition in ('E20') AND l.opportunity_sfid is null AND 
          CASE WHEN l.trial OR COALESCE(lower(split_part(l.company,  ' - ', 2)), ' ') IN ('trial', 'non-prod', 'stage license') 
            OR DATEDIFF('DAY', start_date, expire_date) < 120 
          THEN TRUE 
          ELSE FALSE END THEN 'E20 Trial'
          WHEN l.edition in ('E10') AND ACCOUNT_SFID IS NULL AND l.opportunity_sfid is null and
            CASE WHEN l.trial OR COALESCE(lower(split_part(l.company,  ' - ', 2)), ' ') IN ('trial', 'non-prod', 'stage license') 
            OR DATEDIFF('DAY', start_date, expire_date) < 120 
          THEN TRUE 
          ELSE FALSE END THEN 'E20 Trial'
          WHEN l.edition IS NULL AND 
            CASE WHEN l.trial OR COALESCE(lower(split_part(l.company,  ' - ', 2)), ' ') IN ('trial', 'non-prod', 'stage license') 
            OR DATEDIFF('DAY', start_date, expire_date) < 120
          THEN TRUE 
          ELSE FALSE END AND l.opportunity_sfid IS NULL AND l.account_sfid IS NULL THEN 'E20 Trial'
           ELSE l.edition END    AS edition               
   , l.users              
   , CASE WHEN l.edition in ('E20') AND l.opportunity_sfid is null AND 
          CASE WHEN l.trial OR COALESCE(lower(split_part(l.company,  ' - ', 2)), ' ') IN ('trial', 'non-prod', 'stage license') 
            OR DATEDIFF('DAY', start_date, expire_date) < 120 
          THEN TRUE 
          ELSE FALSE END THEN TRUE
          WHEN l.edition in ('E10') AND ACCOUNT_SFID IS NULL AND l.opportunity_sfid is null and
            CASE WHEN l.trial OR COALESCE(lower(split_part(l.company,  ' - ', 2)), ' ') IN ('trial', 'non-prod', 'stage license') 
            OR DATEDIFF('DAY', start_date, expire_date) < 120 
          THEN TRUE 
          ELSE FALSE END THEN TRUE
          WHEN l.edition IS NULL AND 
            CASE WHEN l.trial OR COALESCE(lower(split_part(l.company,  ' - ', 2)), ' ') IN ('trial', 'non-prod', 'stage license') 
            OR DATEDIFF('DAY', start_date, expire_date) < 120
          THEN TRUE 
          ELSE FALSE END AND l.opportunity_sfid IS NULL AND l.account_sfid IS NULL THEN TRUE
           ELSE FALSE END AS TRIAL                     
   , l.issued_date             
   , l.start_date              
   , l.expire_date             
   , l.license_email           
   , l.contact_sfid            
   , l.account_sfid            
   , l.account_name            
   , l.opportunity_sfid        
   , l.stripeid                
   , l.license_customer_id            
    , l.license_activation_date 
    , l.last_active_date        
    , l.server_activation_date  
    , l.license_priority_rank   
   , l.license_retired_date
   , activity.date as last_server_telemetry
   , MAX(CASE WHEN license_priority_rank = 1 then activity.date else null end) OVER (PARTITION BY customer_id, CASE WHEN l.trial OR COALESCE(lower(split_part(l.company,  ' - ', 2)), ' ') IN ('trial', 'non-prod', 'stage license') 
            OR DATEDIFF('DAY', start_date, expire_date) < 120 
          THEN TRUE 
          ELSE FALSE END) AS last_telemetry_date   
   , SUM(COALESCE(CASE WHEN activity.date >= CURRENT_DATE - INTERVAL '7 DAYS' AND LICENSE_PRIORITY_RANK = 1  
                    THEN activity.active_users 
                    ELSE NULL END, 
                  CASE WHEN activity.date >= CURRENT_DATE - INTERVAL '7 DAYS' AND LICENSE_PRIORITY_RANK = 1  
                    THEN activity.active_users_daily
                    ELSE NULL END)) OVER (PARTITION BY customer_id, CASE WHEN l.trial OR COALESCE(lower(split_part(l.company,  ' - ', 2)), ' ') IN ('trial', 'non-prod', 'stage license') 
            OR DATEDIFF('DAY', start_date, expire_date) < 120 
          THEN TRUE 
          ELSE FALSE END) AS active_users
   , SUM(CASE WHEN activity.date >= CURRENT_DATE - INTERVAL '7 DAYS' AND LICENSE_PRIORITY_RANK = 1 THEN activity.active_users_monthly ELSE NULL END) 
        OVER (PARTITION BY customer_id, CASE WHEN l.trial OR COALESCE(lower(split_part(l.company,  ' - ', 2)), ' ') IN ('trial', 'non-prod', 'stage license') 
            OR DATEDIFF('DAY', start_date, expire_date) < 120 
          THEN TRUE 
          ELSE FALSE END) AS monthly_active_users
   , SUM(CASE WHEN activity.date >= CURRENT_DATE - INTERVAL '7 DAYS' AND LICENSE_PRIORITY_RANK = 1 THEN activity.bot_accounts ELSE NULL END) 
        OVER (PARTITION BY customer_id, CASE WHEN l.trial OR COALESCE(lower(split_part(l.company,  ' - ', 2)), ' ') IN ('trial', 'non-prod', 'stage license') 
            OR DATEDIFF('DAY', start_date, expire_date) < 120 
          THEN TRUE 
          ELSE FALSE END) AS bot_accounts
   , SUM(CASE WHEN activity.date >= CURRENT_DATE - INTERVAL '7 DAYS' AND LICENSE_PRIORITY_RANK = 1 THEN activity.bot_posts_previous_day ELSE NULL END) 
        OVER (PARTITION BY customer_id, CASE WHEN l.trial OR COALESCE(lower(split_part(l.company,  ' - ', 2)), ' ') IN ('trial', 'non-prod', 'stage license') 
            OR DATEDIFF('DAY', start_date, expire_date) < 120 
          THEN TRUE 
          ELSE FALSE END) AS bot_posts_previous_day
   , SUM(CASE WHEN activity.date >= CURRENT_DATE - INTERVAL '7 DAYS'AND LICENSE_PRIORITY_RANK = 1 THEN activity.direct_message_channels ELSE NULL END) 
        OVER (PARTITION BY customer_id, CASE WHEN l.trial OR COALESCE(lower(split_part(l.company,  ' - ', 2)), ' ') IN ('trial', 'non-prod', 'stage license') 
            OR DATEDIFF('DAY', start_date, expire_date) < 120 
          THEN TRUE 
          ELSE FALSE END) AS direct_message_channels
   , SUM(CASE WHEN activity.date >= CURRENT_DATE - INTERVAL '7 DAYS' AND LICENSE_PRIORITY_RANK = 1 THEN activity.incoming_webhooks ELSE NULL END) 
        OVER (PARTITION BY customer_id, CASE WHEN l.trial OR COALESCE(lower(split_part(l.company,  ' - ', 2)), ' ') IN ('trial', 'non-prod', 'stage license') 
            OR DATEDIFF('DAY', start_date, expire_date) < 120 
          THEN TRUE 
          ELSE FALSE END) AS incoming_webhooks
   , SUM(CASE WHEN activity.date >= CURRENT_DATE - INTERVAL '7 DAYS' AND LICENSE_PRIORITY_RANK = 1 THEN activity.outgoing_webhooks ELSE NULL END) 
        OVER (PARTITION BY customer_id, CASE WHEN l.trial OR COALESCE(lower(split_part(l.company,  ' - ', 2)), ' ') IN ('trial', 'non-prod', 'stage license') 
            OR DATEDIFF('DAY', start_date, expire_date) < 120 
          THEN TRUE 
          ELSE FALSE END) AS outgoing_webhooks
   , SUM(CASE WHEN LICENSE_PRIORITY_RANK = 1 THEN activity.posts ELSE NULL END) 
        OVER (PARTITION BY customer_id, CASE WHEN l.trial OR COALESCE(lower(split_part(l.company,  ' - ', 2)), ' ') IN ('trial', 'non-prod', 'stage license') 
            OR DATEDIFF('DAY', start_date, expire_date) < 120 
          THEN TRUE 
          ELSE FALSE END) AS posts
   , SUM(CASE WHEN activity.date >= CURRENT_DATE - INTERVAL '7 DAYS' AND LICENSE_PRIORITY_RANK = 1 THEN activity.posts_previous_day ELSE NULL END) 
        OVER (PARTITION BY customer_id, CASE WHEN l.trial OR COALESCE(lower(split_part(l.company,  ' - ', 2)), ' ') IN ('trial', 'non-prod', 'stage license') 
            OR DATEDIFF('DAY', start_date, expire_date) < 120 
          THEN TRUE 
          ELSE FALSE END) AS posts_previous_day
   , SUM(CASE WHEN activity.date >= CURRENT_DATE - INTERVAL '7 DAYS' AND LICENSE_PRIORITY_RANK = 1 THEN activity.private_channels ELSE NULL END) 
        OVER (PARTITION BY customer_id, CASE WHEN l.trial OR COALESCE(lower(split_part(l.company,  ' - ', 2)), ' ') IN ('trial', 'non-prod', 'stage license') 
            OR DATEDIFF('DAY', start_date, expire_date) < 120 
          THEN TRUE 
          ELSE FALSE END) AS private_channels
   , SUM(CASE WHEN activity.date >= CURRENT_DATE - INTERVAL '7 DAYS' AND LICENSE_PRIORITY_RANK = 1 THEN activity.private_channels_deleted ELSE NULL END) 
        OVER (PARTITION BY customer_id, CASE WHEN l.trial OR COALESCE(lower(split_part(l.company,  ' - ', 2)), ' ') IN ('trial', 'non-prod', 'stage license') 
            OR DATEDIFF('DAY', start_date, expire_date) < 120 
          THEN TRUE 
          ELSE FALSE END) AS private_channels_deleted
   , SUM(CASE WHEN activity.date >= CURRENT_DATE - INTERVAL '7 DAYS' AND LICENSE_PRIORITY_RANK = 1 THEN activity.public_channels ELSE NULL END) 
        OVER (PARTITION BY customer_id, CASE WHEN l.trial OR COALESCE(lower(split_part(l.company,  ' - ', 2)), ' ') IN ('trial', 'non-prod', 'stage license') 
            OR DATEDIFF('DAY', start_date, expire_date) < 120 
          THEN TRUE 
          ELSE FALSE END) AS public_channels
   , SUM(CASE WHEN activity.date >= CURRENT_DATE - INTERVAL '7 DAYS' AND LICENSE_PRIORITY_RANK = 1 THEN activity.public_channels_deleted ELSE NULL END) 
        OVER (PARTITION BY customer_id, CASE WHEN l.trial OR COALESCE(lower(split_part(l.company,  ' - ', 2)), ' ') IN ('trial', 'non-prod', 'stage license') 
            OR DATEDIFF('DAY', start_date, expire_date) < 120 
          THEN TRUE 
          ELSE FALSE END) AS public_channels_deleted
   , SUM(CASE WHEN activity.date >= CURRENT_DATE - INTERVAL '7 DAYS' AND LICENSE_PRIORITY_RANK = 1  THEN activity.registered_deactivated_users ELSE NULL END) 
        OVER (PARTITION BY customer_id, CASE WHEN l.trial OR COALESCE(lower(split_part(l.company,  ' - ', 2)), ' ') IN ('trial', 'non-prod', 'stage license') 
            OR DATEDIFF('DAY', start_date, expire_date) < 120 
          THEN TRUE 
          ELSE FALSE END) AS registered_deactivated_users
   , SUM(CASE WHEN activity.date >= CURRENT_DATE - INTERVAL '7 DAYS' AND LICENSE_PRIORITY_RANK = 1  THEN activity.registered_inactive_users ELSE NULL END) 
        OVER (PARTITION BY customer_id, CASE WHEN l.trial OR COALESCE(lower(split_part(l.company,  ' - ', 2)), ' ') IN ('trial', 'non-prod', 'stage license') 
            OR DATEDIFF('DAY', start_date, expire_date) < 120 
          THEN TRUE 
          ELSE FALSE END) AS registered_inactive_users
   , SUM(CASE WHEN activity.date >= CURRENT_DATE - INTERVAL '7 DAYS' AND LICENSE_PRIORITY_RANK = 1 THEN activity.registered_users ELSE NULL END) 
        OVER (PARTITION BY customer_id, CASE WHEN l.trial OR COALESCE(lower(split_part(l.company,  ' - ', 2)), ' ') IN ('trial', 'non-prod', 'stage license') 
            OR DATEDIFF('DAY', start_date, expire_date) < 120 
          THEN TRUE 
          ELSE FALSE END) AS registered_users
   , SUM(CASE WHEN activity.date >= CURRENT_DATE - INTERVAL '7 DAYS' AND LICENSE_PRIORITY_RANK = 1 THEN activity.slash_commands ELSE NULL END) 
        OVER (PARTITION BY customer_id, CASE WHEN l.trial OR COALESCE(lower(split_part(l.company,  ' - ', 2)), ' ') IN ('trial', 'non-prod', 'stage license') 
            OR DATEDIFF('DAY', start_date, expire_date) < 120 
          THEN TRUE 
          ELSE FALSE END) AS slash_commands
   , SUM(CASE WHEN activity.date >= CURRENT_DATE - INTERVAL '7 DAYS' AND LICENSE_PRIORITY_RANK = 1 THEN activity.teams ELSE NULL END) 
        OVER (PARTITION BY customer_id, CASE WHEN l.trial OR COALESCE(lower(split_part(l.company,  ' - ', 2)), ' ') IN ('trial', 'non-prod', 'stage license') 
            OR DATEDIFF('DAY', start_date, expire_date) < 120 
          THEN TRUE 
          ELSE FALSE END) AS teams
   , SUM(CASE WHEN activity.date >= CURRENT_DATE - INTERVAL '7 DAYS' AND LICENSE_PRIORITY_RANK = 1 THEN activity.guest_accounts ELSE NULL END) 
        OVER (PARTITION BY customer_id, CASE WHEN l.trial OR COALESCE(lower(split_part(l.company,  ' - ', 2)), ' ') IN ('trial', 'non-prod', 'stage license') 
            OR DATEDIFF('DAY', start_date, expire_date) < 120 
          THEN TRUE 
          ELSE FALSE END) AS guest_accounts
   , SUM(CASE WHEN LICENSE_PRIORITY_RANK = 1 AND LICENSE_RETIRED_DATE >= CURRENT_DATE AND LICENSE_RANK = 1 THEN l.users ELSE NULL END)
        OVER (PARTITION BY customer_id, CASE WHEN l.trial OR COALESCE(lower(split_part(l.company,  ' - ', 2)), ' ') IN ('trial', 'non-prod', 'stage license') 
            OR DATEDIFF('DAY', start_date, expire_date) < 120 
          THEN TRUE 
          ELSE FALSE END) AS customer_license_users
FROM license_server_fact l
LEFT JOIN server_activity activity
    ON l.server_id = activity.server_id
{% if is_incremental() %}

WHERE 
  issued_date >= (SELECT MAX(issued_date)::date - INTERVAL '1 DAY' FROM {{this}})
  OR 
  last_active_date::date >= (SELECT MAX(last_active_date::date) - INTERVAL '1 DAY' FROM {{this}})

{% endif %}

QUALIFY ROW_NUMBER() OVER (PARTITION BY L.id ORDER BY ISSUED_DATE) = 1

