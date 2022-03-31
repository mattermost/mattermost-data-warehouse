{{config({
    "materialized": 'table',
    "schema": "blp",
    "unique_key": 'id',
    "tags":'hourly'
  })
}}

with licensed_servers as (
SELECT
    {{ dbt_utils.surrogate_key(['l.license_id', 'l.server_id']) }} as id
  , l.server_id
  , l.license_id
  , MAX(trim(COALESCE(am.company, l.company, s.company))) AS company
  , MAX(COALESCE(am.edition, l.edition)) AS edition
  , MAX(l.users)   AS users
  , l.trial
  , MIN(l.issued_date::date) AS issued_date
  , MIN(l.start_date::date) AS start_date
  , MAX(l.expire_date::date) AS expire_date
  , MAX(trim(lower(l.license_email))) AS license_email
  , MAX(COALESCE(am.contact_sfid, l.contact_sfid)) AS contact_sfid
  , MAX(COALESCE(am.account_sfid, l.account_sfid, s.account_sfid)) AS account_sfid
  , MAX(COALESCE(am.account_name, l.account_name, s.account_name)) AS account_name
  , MAX(am.opportunity_sfid) AS opportunity_sfid
  , l.stripeid
  , l.customer_id
  , MIN(l.license_activation_date) AS license_activation_date
  , MAX(l.timestamp)  AS last_active_date
  , MIN(s.first_active_date) AS server_activation_date
FROM {{ ref('licenses') }} l
LEFT JOIN {{ ref('server_fact') }} s
  ON l.server_id = s.server_id
LEFT JOIN  {{ ref('account_mapping') }} am
  ON l.license_id = am.license_id
WHERE l.server_id IS NOT NULL
AND l.license_id <> '16tfkttgktgdmb5m8xakqncx3c'
AND l.issued_date::DATE <= CURRENT_DATE
GROUP BY 1, 2, 3, 7, 16, 17
)
select * from licensed_servers

