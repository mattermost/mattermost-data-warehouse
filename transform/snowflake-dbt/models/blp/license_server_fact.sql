{{config({
    "materialized": 'incremental',
    "schema": "blp",
    "unique_key": 'id',
    "tags":'nightly'
  })
}}

with licensed_servers as (
SELECT
    {{ dbt_utils.surrogate_key('l.server_id', 'l.license_id') }} as id
  , l.server_id
  , l.license_id
  , MAX(COALESCE(l.company, s.company)) AS company
  , MAX(l.edition) AS edition
  , MAX(l.users)   AS users
  , l.trial
  , l.issued_date
  , l.start_date
  , l.expire_date
  , MAX(l.license_email) AS license_email
  , MAX(l.contact_sfid) AS contact_sfid
  , MAX(COALESCE(l.account_sfid, s.account_sfid)) AS account_sfid
  , MAX(COALESCE(l.account_name, s.account_name)) AS account_name
  , l.stripeid
  , l.customer_id
  , l.number
  , MIN(l.license_activation_date) AS license_activation_date
  , MAX(l.timestamp)  AS last_active_date
  , MIN(s.first_active_date) AS server_activation_date
FROM {{ ref('licenses') }} l
LEFT JOIN {{ ref('server_fact') }} s
  ON l.server_id = s.server_id
WHERE l.server_id IS NOT NULL
GROUP BY 1, 2, 3, 7, 8, 9, 10, 15, 16, 17
),

licensed_window AS (
  SELECT
    id
  , server_id
  , license_id
  , COALESCE(company
            , MAX(company) OVER (PARTITION BY COALESCE(server_id
                                                      , customer_id
                                                      , account_sfid
                                                      , contact_sfid
                                                      , CASE WHEN SPLIT_PART(lower(license_email), '@', 2) NOT LIKE '%mattermost%' 
                                                          THEN SPLIT_PART(lower(license_email), '@', 2)
                                                        ELSE contact_sfid END
                                                      , license_id))
            , MAX(company) OVER (PARTITION BY COALESCE(customer_id
                                                      , account_sfid
                                                      , contact_sfid
                                                      , CASE WHEN SPLIT_PART(lower(license_email), '@', 2) NOT LIKE '%mattermost%' 
                                                          THEN SPLIT_PART(lower(license_email), '@', 2)
                                                        ELSE contact_sfid END
                                                      , license_id))
            , MAX(company) OVER (PARTITION BY COALESCE(account_sfid
                                                      , contact_sfid
                                                      , CASE WHEN SPLIT_PART(lower(license_email), '@', 2) NOT LIKE '%mattermost%' 
                                                          THEN SPLIT_PART(lower(license_email), '@', 2)
                                                        ELSE contact_sfid END
                                                      , license_id))
            , MAX(company) OVER (PARTITION BY COALESCE(contact_sfid
                                                      , CASE WHEN SPLIT_PART(lower(license_email), '@', 2) NOT LIKE '%mattermost%' 
                                                          THEN SPLIT_PART(lower(license_email), '@', 2)
                                                        ELSE contact_sfid END
                                                      , license_id))
            , MAX(company) OVER (PARTITION BY COALESCE(CASE WHEN SPLIT_PART(lower(license_email), '@', 2) NOT LIKE '%mattermost%' 
                                                          THEN SPLIT_PART(lower(license_email), '@', 2)
                                                        ELSE contact_sfid END
                                                      , license_id))) as company
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
            , MAX(account_sfid) OVER (PARTITION BY COALESCE(server_id
                                                            , customer_id
                                                            , contact_sfid
                                                            , company
                                                            , CASE WHEN SPLIT_PART(lower(license_email), '@', 2) NOT LIKE '%mattermost%' 
                                                                THEN SPLIT_PART(lower(license_email), '@', 2)
                                                              ELSE contact_sfid END
                                                            , license_id))
            , MAX(account_sfid) OVER (PARTITION BY COALESCE(customer_id
                                                            , contact_sfid
                                                            , company
                                                            , CASE WHEN SPLIT_PART(lower(license_email), '@', 2) NOT LIKE '%mattermost%' 
                                                                THEN SPLIT_PART(lower(license_email), '@', 2)
                                                              ELSE contact_sfid END
                                                            , license_id))
            , MAX(account_sfid) OVER (PARTITION BY COALESCE(contact_sfid
                                                            , company
                                                            , CASE WHEN SPLIT_PART(lower(license_email), '@', 2) NOT LIKE '%mattermost%' 
                                                                THEN SPLIT_PART(lower(license_email), '@', 2)
                                                              ELSE contact_sfid END
                                                            , license_id))
            , MAX(account_sfid) OVER (PARTITION BY COALESCE(company
                                                            , CASE WHEN SPLIT_PART(lower(license_email), '@', 2) NOT LIKE '%mattermost%' 
                                                                THEN SPLIT_PART(lower(license_email), '@', 2)
                                                              ELSE contact_sfid END
                                                            , license_id))
            , MAX(account_sfid) OVER (PARTITION BY COALESCE(CASE WHEN SPLIT_PART(lower(license_email), '@', 2) NOT LIKE '%mattermost%' 
                                                                THEN SPLIT_PART(lower(license_email), '@', 2)
                                                              ELSE contact_sfid END
                                                            , license_id))) AS account_sfid
  , COALESCE(account_name
            , MAX(account_name) OVER (PARTITION BY COALESCE(server_id
                                                            , customer_id
                                                            , contact_sfid
                                                            , company
                                                            , CASE WHEN SPLIT_PART(lower(license_email), '@', 2) NOT LIKE '%mattermost%' 
                                                                THEN SPLIT_PART(lower(license_email), '@', 2)
                                                              ELSE contact_sfid END
                                                            , license_id))
            , MAX(account_name) OVER (PARTITION BY COALESCE(customer_id
                                                            , contact_sfid
                                                            , company
                                                            , CASE WHEN SPLIT_PART(lower(license_email), '@', 2) NOT LIKE '%mattermost%' 
                                                                THEN SPLIT_PART(lower(license_email), '@', 2)
                                                              ELSE contact_sfid END
                                                            , license_id))
            , MAX(account_name) OVER (PARTITION BY COALESCE(contact_sfid
                                                            , company
                                                            , CASE WHEN SPLIT_PART(lower(license_email), '@', 2) NOT LIKE '%mattermost%' 
                                                                THEN SPLIT_PART(lower(license_email), '@', 2)
                                                              ELSE contact_sfid END
                                                            , license_id))
            , MAX(account_name) OVER (PARTITION BY COALESCE(company
                                                            , CASE WHEN SPLIT_PART(lower(license_email), '@', 2) NOT LIKE '%mattermost%' 
                                                                THEN SPLIT_PART(lower(license_email), '@', 2)
                                                              ELSE contact_sfid END
                                                            , license_id))
            , MAX(account_name) OVER (PARTITION BY COALESCE(CASE WHEN SPLIT_PART(lower(license_email), '@', 2) NOT LIKE '%mattermost%' 
                                                                THEN SPLIT_PART(lower(license_email), '@', 2)
                                                              ELSE contact_sfid END
                                                            , license_id))) AS account_name
  , stripeid
  , customer_id
  , number
  , license_activation_date
  , last_active_date
  , server_activation_date
  FROM licensed_servers
),

nonactivated_licenses as (
  SELECT
    {{ dbt_utils.surrogate_key('l.server_id', 'l.license_id') }} as id
  , l.server_id
  , l.license_id
  , MAX(l.company) AS company
  , MAX(l.edition) AS edition
  , MAX(l.users)   AS users
  , l.trial
  , l.issued_date
  , l.start_date
  , l.expire_date
  , MAX(l.license_email) AS license_email
  , MAX(l.contact_sfid) AS contact_sfid
  , MAX(l.account_sfid) AS account_sfid
  , MAX(l.account_name) AS account_name
  , l.stripeid
  , l.customer_id
  , l.number
  , MIN(l.license_activation_date) AS license_activation_date
  , MAX(l.timestamp)  AS last_active_date
  , MIN(NULL) AS server_activation_date
  FROM {{ ref('licenses') }} l
  LEFT JOIN licensed_servers s
    ON l.license_id = s.license_id
  WHERE s.license_id is null
  GROUP BY 1, 2, 3, 7, 8, 9, 10, 15, 16, 17
),

nonactivated_license_window as (
  SELECT
    id
  , server_id
  , license_id
  , COALESCE(company
            , MAX(company) OVER (PARTITION BY COALESCE(server_id
                                                      , customer_id
                                                      , account_sfid
                                                      , contact_sfid
                                                      , CASE WHEN SPLIT_PART(lower(license_email), '@', 2) NOT LIKE '%mattermost%' 
                                                          THEN SPLIT_PART(lower(license_email), '@', 2)
                                                        ELSE contact_sfid END
                                                      , license_id))
            , MAX(company) OVER (PARTITION BY COALESCE(customer_id
                                                      , account_sfid
                                                      , contact_sfid
                                                      , CASE WHEN SPLIT_PART(lower(license_email), '@', 2) NOT LIKE '%mattermost%' 
                                                          THEN SPLIT_PART(lower(license_email), '@', 2)
                                                        ELSE contact_sfid END
                                                      , license_id))
            , MAX(company) OVER (PARTITION BY COALESCE(account_sfid
                                                      , contact_sfid
                                                      , CASE WHEN SPLIT_PART(lower(license_email), '@', 2) NOT LIKE '%mattermost%' 
                                                          THEN SPLIT_PART(lower(license_email), '@', 2)
                                                        ELSE contact_sfid END
                                                      , license_id))
            , MAX(company) OVER (PARTITION BY COALESCE(contact_sfid
                                                      , CASE WHEN SPLIT_PART(lower(license_email), '@', 2) NOT LIKE '%mattermost%' 
                                                          THEN SPLIT_PART(lower(license_email), '@', 2)
                                                        ELSE contact_sfid END
                                                      , license_id))
            , MAX(company) OVER (PARTITION BY COALESCE(CASE WHEN SPLIT_PART(lower(license_email), '@', 2) NOT LIKE '%mattermost%' 
                                                          THEN SPLIT_PART(lower(license_email), '@', 2)
                                                        ELSE contact_sfid END
                                                      , license_id))) as company
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
            , MAX(account_sfid) OVER (PARTITION BY COALESCE(server_id
                                                            , customer_id
                                                            , contact_sfid
                                                            , company
                                                            , CASE WHEN SPLIT_PART(lower(license_email), '@', 2) NOT LIKE '%mattermost%' 
                                                                THEN SPLIT_PART(lower(license_email), '@', 2)
                                                              ELSE contact_sfid END
                                                            , license_id))
            , MAX(account_sfid) OVER (PARTITION BY COALESCE(customer_id
                                                            , contact_sfid
                                                            , company
                                                            , CASE WHEN SPLIT_PART(lower(license_email), '@', 2) NOT LIKE '%mattermost%' 
                                                                THEN SPLIT_PART(lower(license_email), '@', 2)
                                                              ELSE contact_sfid END
                                                            , license_id))
            , MAX(account_sfid) OVER (PARTITION BY COALESCE(contact_sfid
                                                            , company
                                                            , CASE WHEN SPLIT_PART(lower(license_email), '@', 2) NOT LIKE '%mattermost%' 
                                                                THEN SPLIT_PART(lower(license_email), '@', 2)
                                                              ELSE contact_sfid END
                                                            , license_id))
            , MAX(account_sfid) OVER (PARTITION BY COALESCE(company
                                                            , CASE WHEN SPLIT_PART(lower(license_email), '@', 2) NOT LIKE '%mattermost%' 
                                                                THEN SPLIT_PART(lower(license_email), '@', 2)
                                                              ELSE contact_sfid END
                                                            , license_id))
            , MAX(account_sfid) OVER (PARTITION BY COALESCE(CASE WHEN SPLIT_PART(lower(license_email), '@', 2) NOT LIKE '%mattermost%' 
                                                                THEN SPLIT_PART(lower(license_email), '@', 2)
                                                              ELSE contact_sfid END
                                                            , license_id))) AS account_sfid
, COALESCE(account_name
            , MAX(account_name) OVER (PARTITION BY COALESCE(server_id
                                                            , customer_id
                                                            , contact_sfid
                                                            , company
                                                            , CASE WHEN SPLIT_PART(lower(license_email), '@', 2) NOT LIKE '%mattermost%' 
                                                                THEN SPLIT_PART(lower(license_email), '@', 2)
                                                              ELSE contact_sfid END
                                                            , license_id))
            , MAX(account_name) OVER (PARTITION BY COALESCE(customer_id
                                                            , contact_sfid
                                                            , company
                                                            , CASE WHEN SPLIT_PART(lower(license_email), '@', 2) NOT LIKE '%mattermost%' 
                                                                THEN SPLIT_PART(lower(license_email), '@', 2)
                                                              ELSE contact_sfid END
                                                            , license_id))
            , MAX(account_name) OVER (PARTITION BY COALESCE(contact_sfid
                                                            , company
                                                            , CASE WHEN SPLIT_PART(lower(license_email), '@', 2) NOT LIKE '%mattermost%' 
                                                                THEN SPLIT_PART(lower(license_email), '@', 2)
                                                              ELSE contact_sfid END
                                                            , license_id))
            , MAX(account_name) OVER (PARTITION BY COALESCE(company
                                                            , CASE WHEN SPLIT_PART(lower(license_email), '@', 2) NOT LIKE '%mattermost%' 
                                                                THEN SPLIT_PART(lower(license_email), '@', 2)
                                                              ELSE contact_sfid END
                                                            , license_id))
            , MAX(account_name) OVER (PARTITION BY COALESCE(CASE WHEN SPLIT_PART(lower(license_email), '@', 2) NOT LIKE '%mattermost%' 
                                                                THEN SPLIT_PART(lower(license_email), '@', 2)
                                                              ELSE contact_sfid END
                                                            , license_id))) AS account_name
  , stripeid
  , customer_id
  , number
  , license_activation_date
  , last_active_date
  , server_activation_date
  FROM nonactivated_licenses
),

license_server_fact as (
  SELECT *
  FROM licensed_window
  
  UNION ALL

  SELECT *
  FROM nonactivated_license_window
)

SELECT 
        id
      , server_id
      , license_id
      , COALESCE(company
                , MAX(company) OVER (PARTITION BY COALESCE(server_id
                                                          , customer_id
                                                          , account_sfid
                                                          , contact_sfid
                                                          , CASE WHEN SPLIT_PART(lower(license_email), '@', 2) NOT LIKE '%mattermost%' 
                                                              THEN SPLIT_PART(lower(license_email), '@', 2)
                                                            ELSE contact_sfid END
                                                          , license_id))
                , MAX(company) OVER (PARTITION BY COALESCE(customer_id
                                                          , account_sfid
                                                          , contact_sfid
                                                          , CASE WHEN SPLIT_PART(lower(license_email), '@', 2) NOT LIKE '%mattermost%' 
                                                              THEN SPLIT_PART(lower(license_email), '@', 2)
                                                            ELSE contact_sfid END
                                                          , license_id))
                , MAX(company) OVER (PARTITION BY COALESCE(account_sfid
                                                          , contact_sfid
                                                          , CASE WHEN SPLIT_PART(lower(license_email), '@', 2) NOT LIKE '%mattermost%' 
                                                              THEN SPLIT_PART(lower(license_email), '@', 2)
                                                            ELSE contact_sfid END
                                                          , license_id))
                , MAX(company) OVER (PARTITION BY COALESCE(contact_sfid
                                                          , CASE WHEN SPLIT_PART(lower(license_email), '@', 2) NOT LIKE '%mattermost%' 
                                                              THEN SPLIT_PART(lower(license_email), '@', 2)
                                                            ELSE contact_sfid END
                                                          , license_id))
                , MAX(company) OVER (PARTITION BY COALESCE(CASE WHEN SPLIT_PART(lower(license_email), '@', 2) NOT LIKE '%mattermost%' 
                                                              THEN SPLIT_PART(lower(license_email), '@', 2)
                                                            ELSE contact_sfid END
                                                          , license_id))) as company
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
                , MAX(account_sfid) OVER (PARTITION BY COALESCE(server_id
                                                               , customer_id
                                                               , contact_sfid
                                                               , company
                                                               , CASE WHEN SPLIT_PART(lower(license_email), '@', 2) NOT LIKE '%mattermost%' 
                                                                    THEN SPLIT_PART(lower(license_email), '@', 2)
                                                                  ELSE contact_sfid END
                                                                , license_id))
                , MAX(account_sfid) OVER (PARTITION BY COALESCE(customer_id
                                                               , contact_sfid
                                                               , company
                                                               , CASE WHEN SPLIT_PART(lower(license_email), '@', 2) NOT LIKE '%mattermost%' 
                                                                    THEN SPLIT_PART(lower(license_email), '@', 2)
                                                                  ELSE contact_sfid END
                                                                , license_id))
                , MAX(account_sfid) OVER (PARTITION BY COALESCE(contact_sfid
                                                               , company
                                                               , CASE WHEN SPLIT_PART(lower(license_email), '@', 2) NOT LIKE '%mattermost%' 
                                                                    THEN SPLIT_PART(lower(license_email), '@', 2)
                                                                  ELSE contact_sfid END
                                                                , license_id))
                , MAX(account_sfid) OVER (PARTITION BY COALESCE(company
                                                               , CASE WHEN SPLIT_PART(lower(license_email), '@', 2) NOT LIKE '%mattermost%' 
                                                                    THEN SPLIT_PART(lower(license_email), '@', 2)
                                                                  ELSE contact_sfid END
                                                                , license_id))
                , MAX(account_sfid) OVER (PARTITION BY COALESCE(CASE WHEN SPLIT_PART(lower(license_email), '@', 2) NOT LIKE '%mattermost%' 
                                                                    THEN SPLIT_PART(lower(license_email), '@', 2)
                                                                  ELSE contact_sfid END
                                                                , license_id))) AS account_sfid
    , COALESCE(account_name
                , MAX(account_name) OVER (PARTITION BY COALESCE(server_id
                                                               , customer_id
                                                               , contact_sfid
                                                               , company
                                                               , CASE WHEN SPLIT_PART(lower(license_email), '@', 2) NOT LIKE '%mattermost%' 
                                                                    THEN SPLIT_PART(lower(license_email), '@', 2)
                                                                  ELSE contact_sfid END
                                                                , license_id))
                , MAX(account_name) OVER (PARTITION BY COALESCE(customer_id
                                                               , contact_sfid
                                                               , company
                                                               , CASE WHEN SPLIT_PART(lower(license_email), '@', 2) NOT LIKE '%mattermost%' 
                                                                    THEN SPLIT_PART(lower(license_email), '@', 2)
                                                                  ELSE contact_sfid END
                                                                , license_id))
                , MAX(account_name) OVER (PARTITION BY COALESCE(contact_sfid
                                                               , company
                                                               , CASE WHEN SPLIT_PART(lower(license_email), '@', 2) NOT LIKE '%mattermost%' 
                                                                    THEN SPLIT_PART(lower(license_email), '@', 2)
                                                                  ELSE contact_sfid END
                                                                , license_id))
                , MAX(account_name) OVER (PARTITION BY COALESCE(company
                                                               , CASE WHEN SPLIT_PART(lower(license_email), '@', 2) NOT LIKE '%mattermost%' 
                                                                    THEN SPLIT_PART(lower(license_email), '@', 2)
                                                                  ELSE contact_sfid END
                                                                , license_id))
                , MAX(account_name) OVER (PARTITION BY COALESCE(CASE WHEN SPLIT_PART(lower(license_email), '@', 2) NOT LIKE '%mattermost%' 
                                                                    THEN SPLIT_PART(lower(license_email), '@', 2)
                                                                  ELSE contact_sfid END
                                                                , license_id))) AS account_name
      , stripeid
      , customer_id
      , number
      , license_activation_date
      , last_active_date
      , server_activation_date
       , ROW_NUMBER() OVER (PARTITION BY COALESCE(server_id, license_id) ORDER BY trial DESC, license_activation_date desc) AS license_priority_rank
       , CASE WHEN COALESCE((lead(start_date, 4) OVER (PARTITION BY COALESCE(server_id, license_id) ORDER BY issued_date))::DATE, expire_date::DATE + INTERVAL '1 DAY') <= expire_date 
            THEN (lead(start_date, 4) OVER (PARTITION BY COALESCE(server_id, license_id) ORDER BY issued_date))::DATE - interval '1 day'
          WHEN COALESCE((lead(start_date, 3) OVER (PARTITION BY COALESCE(server_id, license_id) ORDER BY issued_date))::DATE, expire_date::DATE + INTERVAL '1 DAY') <= expire_date 
            THEN (lead(start_date, 3) OVER (PARTITION BY COALESCE(server_id, license_id) ORDER BY issued_date))::DATE - interval '1 day'
          WHEN COALESCE((lead(start_date, 2) OVER (PARTITION BY COALESCE(server_id, license_id) ORDER BY issued_date))::DATE, expire_date::DATE + INTERVAL '1 DAY') <= expire_date 
            THEN (lead(start_date, 2) OVER (PARTITION BY COALESCE(server_id, license_id) ORDER BY issued_date))::DATE - interval '1 day'
          WHEN COALESCE((lead(start_date, 1) OVER (PARTITION BY COALESCE(server_id, license_id) ORDER BY issued_date))::DATE, expire_date::DATE + INTERVAL '1 DAY') <= expire_date
            THEN (lead(start_date, 1) OVER (PARTITION BY COALESCE(server_id, license_id) ORDER BY issued_date))::DATE - interval '1 day'
          ELSE expire_date END as license_retired_date
FROM license_server_fact
{% if is_incremental() %}

WHERE (
        (last_active_date >= (SELECT MAX(last_active_date) FROM {{this}}))
         OR 
        (issued_date >= (SELECT MAX(issued_date) FROM {{this}}))
      )

{% endif %}