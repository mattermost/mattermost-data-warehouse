{{config({
    "materialized": 'table',
    "schema": "blp",
    "unique_key": 'id',
    "tags":'hourly'
  })
}}


with 
max_sku AS (
  SELECT DISTINCT
      s1.subscription
    , MAX(s1.plan_name) AS plan_name
  FROM {{ ref('subscription_items')}} s1
  JOIN (
    SELECT
        subscription
      , MAX(plan_amount) as max_amount
    FROM {{ ref('subscription_items')}}
    GROUP BY 1
  ) s2
      ON s1.subscription = s2.subscription
      AND s1.plan_amount = s2.max_amount
  GROUP BY 1
),

cloud_subscriptions AS (
  SELECT 
      {{ dbt_utils.surrogate_key(['s.cws_installation', 'coalesce(sf.server_id, server.user_id)'])}}               AS id
    , COALESCE(sf.server_id, server.user_id)                       AS server_id
    , s.cws_installation                                           AS license_id
    , COALESCE(am.account_sfid, c.cws_customer)                                               AS customer_id
    , COALESCE(am.account_name, INITCAP(SPLIT_PART(replace(s.cws_dns, '-', ' '), '.', 1)))    AS customer_name
    , INITCAP(SPLIT_PART(replace(s.cws_dns, '-', ' '), '.', 1))    AS company
    , COALESCE(s.edition, am.edition, ms.plan_name, 'Mattermost Cloud')       AS edition
    , s.quantity                                                   AS users
    , FALSE                                                        AS trial
    , MIN(s.created::DATE)                                         AS issued_date
    , COALESCE(MIN(sf.first_active_date::date), 
               MIN(server.timestamp::date), 
               MIN(s.current_period_start::DATE))                  AS start_date
    , MAX(s.current_period_end::DATE)                              AS expire_date
    , c.email                                                      AS license_email
    , MAX(COALESCE(am.contact_sfid, NULL))                                                         AS contact_sfid
    , MAX(COALESCE(am.account_sfid, NULL))                                                         AS account_sfid
    , MAX(COALESCE(am.account_name, NULL))                                                        AS account_name
    , MAX(COALESCE(am.opportunity_sfid, NULL))                                                     AS opportunity_sfid
    , c.id                                                         AS stripeid
    , c.cws_customer                                               AS license_customer_id
    , s.created::TIMESTAMP                                         AS license_activation_date
    , COALESCE(MAX(sf.last_active_date::TIMESTAMP), 
        MAX(server.timestamp::TIMESTAMP))                          AS last_active_date
    , COALESCE(MIN(sf.first_active_date::DATE),
        MIN(server.timestamp::DATE))                               AS server_activation_date
    , 1                                                            AS license_rank
    , 1                                                            AS license_priority_rank
    , s.current_period_end::DATE                                   AS license_retired_date
  FROM {{ ref('subscriptions') }}             s
        LEFT JOIN {{ ref('customers') }}       c
                  ON s.customer = c.id
        LEFT JOIN {{ ref('server_fact') }} sf
                  ON s.cws_installation = sf.installation_id
        LEFT JOIN {{ source('mm_telemetry_prod', 'server') }} server
                  ON s.cws_installation = server.context_traits_installationid
                      AND server.context_traits_installationid IS NOT NULL
        LEFT JOIN max_sku ms
                  ON s.id = ms.subscription
        LEFT JOIN {{ ref('account_mapping') }} am
                  ON s.cws_installation = am.license_id
  WHERE s.cws_installation IS NOT NULL
  AND s.created::DATE <= CURRENT_DATE
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 13, 18, 19, 20
  , 23, 24, 25
) 
select * from cloud_subscriptions


