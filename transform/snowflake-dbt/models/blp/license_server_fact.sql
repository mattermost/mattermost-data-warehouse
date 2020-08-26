{{config({
    "materialized": 'incremental',
    "schema": "blp",
    "unique_key": 'id',
    "tags":'nightly'
  })
}}

with licensed_servers as (
SELECT
    {{ dbt_utils.surrogate_key('server_id', 'license_id') }} as id
  , server_id
  , license_id
  , company
  , MAX(edition) AS edition
  , MAX(users)   AS users
  , trial
  , issued_date
  , start_date
  , expire_date
  , MAX(license_email) AS license_email
  , MAX(contact_sfid) AS contact_sfid
  , MAX(account_sfid) AS account_sfid
  , MAX(account_name) AS account_name
  , stripeid
  , customer_id
  , number
  , MIN(license_activation_date) AS license_activation_date
  , MAX(timestamp)  AS last_active_date
FROM {{ ref('licenses') }}
WHERE server_id IS NOT NULL
GROUP BY 1, 2, 3, 4, 7, 8, 9, 10, 15, 16, 17
),

nonactivated_licenses as (
  SELECT
    {{ dbt_utils.surrogate_key('l.server_id', 'l.license_id') }} as id
  , l.server_id
  , l.license_id
  , l.company
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
  FROM {{ ref('licenses') }} l
  LEFT JOIN licensed_servers s
    ON l.license_id = s.license_id
  WHERE s.license_id is null
  GROUP BY 1, 2, 3, 4, 7, 8, 9, 10, 15, 16, 17
),

nonactivated_license_window as (
  SELECT
    id
  , server_id
  , license_id
  , COALESCE(company, MAX(company) OVER (PARTITION BY license_id)) as company
  , edition
  , users
  , trial
  , issued_date
  , start_date
  , expire_date
  , license_email
  , COALESCE(contact_sfid, MAX(contact_sfid) OVER (PARTITION BY license_email)) as contact_sfid
  , account_sfid
  , account_name
  , stripeid
  , customer_id
  , number
  , license_activation_date
  , last_active_date
  FROM nonactivated_licenses
),

license_server_fact as (
  SELECT *
  FROM licensed_servers
  
  UNION ALL

  SELECT *
  FROM nonactivated_license_window
)

SELECT *
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