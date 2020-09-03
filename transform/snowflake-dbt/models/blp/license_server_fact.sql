{{config({
    "materialized": 'incremental',
    "schema": "blp",
    "unique_key": 'id',
    "tags":'nightly'
  })
}}

with account_mapping as (
  SELECT 
      elm.account_sfid
    , a.name as account_name
    , elm.licenseid as license_id
    , elm.opportunity_sfid
  FROM (
        SELECT
            COALESCE(elm.account_sfid, lo.account_sfid) AS account_sfid
          , COALESCE(elm.opportunity_sfid, lo.opportunity_sfid) AS opportunity_sfid
          , COALESCE(trim(elm.licenseid), trim(lo.licenseid))   AS licenseid
        FROM {{ ref('enterprise_license_mapping') }} elm
        FULL OUTER JOIN {{ ref('license_overview') }} lo
          ON trim(elm.licenseid) = trim(lo.licenseid)
        GROUP BY 1, 2, 3
      ) elm
  LEFT JOIN {{ source('orgm', 'account') }} a
      ON elm.account_sfid = a.sfid
  GROUP BY 1, 2, 3, 4
), 

licensed_servers as (
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
  , MAX(COALESCE(am.account_sfid, l.account_sfid, s.account_sfid)) AS account_sfid
  , MAX(COALESCE(am.account_name, l.account_name, s.account_name)) AS account_name
  , MAX(am.opportunity_sfid) AS opportunity_sfid
  , l.stripeid
  , l.customer_id
  , l.number
  , MIN(l.license_activation_date) AS license_activation_date
  , MAX(l.timestamp)  AS last_active_date
  , MIN(s.first_active_date) AS server_activation_date
FROM {{ ref('licenses') }} l
LEFT JOIN {{ ref('server_fact') }} s
  ON l.server_id = s.server_id
LEFT JOIN account_mapping am
  ON l.license_id = am.license_id
WHERE l.server_id IS NOT NULL
GROUP BY 1, 2, 3, 7, 8, 9, 10, 16, 17, 18
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
  , MAX(COALESCE(am.account_sfid, l.account_sfid)) AS account_sfid
  , MAX(COALESCE(am.account_name, l.account_name)) AS account_name
  , MAX(am.opportunity_sfid) AS opportunity_sfid
  , l.stripeid
  , l.customer_id
  , l.number
  , MIN(l.license_activation_date) AS license_activation_date
  , MAX(l.timestamp)  AS last_active_date
  , MIN(NULL) AS server_activation_date
  FROM {{ ref('licenses') }} l
  LEFT JOIN licensed_servers s
    ON l.license_id = s.license_id
  LEFT JOIN account_mapping am
    ON l.license_id = am.license_id       
  WHERE s.license_id is null
  GROUP BY 1, 2, 3, 7, 8, 9, 10, 16, 17, 18
),

license_server_fact as (
  SELECT *
  FROM licensed_servers
  
  UNION ALL

  SELECT *
  FROM nonactivated_licenses
)

SELECT 
        id
      , server_id
      , license_id
      , COALESCE(
                COALESCE(account_sfid
                      , MAX(account_sfid) OVER (PARTITION BY COALESCE(server_id
                                                                    , license_id))
                      , MAX(account_sfid) OVER (PARTITION BY COALESCE(customer_id
                                                                    , license_id))
                      , MAX(account_sfid) OVER (PARTITION BY COALESCE(lower(company)
                                                                    , license_id))
                      , MAX(account_sfid) OVER (PARTITION BY COALESCE(contact_sfid
                                                                    , license_id))
                      , MAX(account_sfid) OVER (PARTITION BY COALESCE(contact_sfid
                                                                    , license_id))
                      , MAX(account_sfid) OVER (PARTITION BY COALESCE(CASE WHEN SPLIT_PART(lower(license_email), '@', 2) NOT IN 
                                                                          (SELECT DOMAIN_NAME FROM {{ source('util', 'public_domains')}} GROUP BY 1) 
                                                                          THEN SPLIT_PART(lower(license_email), '@', 2)
                                                                        ELSE contact_sfid END
                                                                      , license_id)))
              , customer_id
      )                                                                               AS customer_id
      , COALESCE(
                 COALESCE(account_name
                        , MAX(account_name) OVER (PARTITION BY COALESCE(server_id
                                                                      , license_id))
                        , MAX(account_name) OVER (PARTITION BY COALESCE(customer_id
                                                                      , license_id))
                        , MAX(account_name) OVER (PARTITION BY COALESCE(lower(company)
                                                                       , license_id))
                        , MAX(account_name) OVER (PARTITION BY COALESCE(contact_sfid
                                                                      , license_id))
                        , MAX(account_name) OVER (PARTITION BY COALESCE(CASE WHEN SPLIT_PART(lower(license_email), '@', 2) NOT IN 
                                                                          (SELECT DOMAIN_NAME FROM {{ source('util', 'public_domains')}} GROUP BY 1) 
                                                                            THEN SPLIT_PART(lower(license_email), '@', 2)
                                                                          ELSE contact_sfid END
                                                                        , license_id)))
                , COALESCE(company
                        , MAX(company) OVER (PARTITION BY COALESCE(server_id
                                                                  , license_id))
                        , MAX(company) OVER (PARTITION BY COALESCE(customer_id
                                                                  , license_id))
                        , MAX(company) OVER (PARTITION BY COALESCE(account_sfid
                                                                  , license_id))
                        , MAX(company) OVER (PARTITION BY COALESCE(contact_sfid
                                                                  , license_id))
                        , MAX(company) OVER (PARTITION BY COALESCE(CASE WHEN SPLIT_PART(lower(license_email), '@', 2) NOT IN 
                                                                          (SELECT DOMAIN_NAME FROM {{ source('util', 'public_domains')}} GROUP BY 1) 
                                                                      THEN SPLIT_PART(lower(license_email), '@', 2)
                                                                    ELSE contact_sfid END
                                                                  , license_id)))
      )                                                                                     AS customer_name
      , COALESCE(company
                , MAX(company) OVER (PARTITION BY COALESCE(server_id
                                                          , license_id))
                , MAX(company) OVER (PARTITION BY COALESCE(customer_id
                                                          , license_id))
                , MAX(company) OVER (PARTITION BY COALESCE(account_sfid
                                                          , license_id))
                , MAX(company) OVER (PARTITION BY COALESCE(contact_sfid
                                                          , license_id))
                , MAX(company) OVER (PARTITION BY COALESCE(CASE WHEN SPLIT_PART(lower(license_email), '@', 2) NOT IN 
                                                                          (SELECT DOMAIN_NAME FROM {{ source('util', 'public_domains')}} GROUP BY 1) 
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
                                                               , license_id))
                , MAX(account_sfid) OVER (PARTITION BY COALESCE(customer_id
                                                               , license_id))
                , MAX(account_sfid) OVER (PARTITION BY COALESCE(lower(company)
                                                               , license_id))
                , MAX(account_sfid) OVER (PARTITION BY COALESCE(contact_sfid
                                                               , license_id))
                , MAX(account_sfid) OVER (PARTITION BY COALESCE(contact_sfid
                                                               , license_id))
                , MAX(account_sfid) OVER (PARTITION BY COALESCE(CASE WHEN SPLIT_PART(lower(license_email), '@', 2) NOT IN 
                                                                          (SELECT DOMAIN_NAME FROM {{ source('util', 'public_domains')}} GROUP BY 1) 
                                                                    THEN SPLIT_PART(lower(license_email), '@', 2)
                                                                  ELSE contact_sfid END
                                                                , license_id))) AS account_sfid
    , COALESCE(account_name
                , MAX(account_name) OVER (PARTITION BY COALESCE(server_id
                                                               , license_id))
                , MAX(account_name) OVER (PARTITION BY COALESCE(customer_id
                                                               , license_id))
                , MAX(account_name) OVER (PARTITION BY COALESCE(lower(company)
                                                               , license_id))
                , MAX(account_name) OVER (PARTITION BY COALESCE(contact_sfid
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