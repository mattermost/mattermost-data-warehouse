{{config({
    "materialized": 'incremental',
    "schema": "blp",
    "unique_key": 'id',
    "tags":'nightly'
  })
}}

with license_server_fact as (
SELECT
    {{ dbt_utils.surrogate_key('server_id', 'license_id') }} as id
  , server_id
  , license_id
  , company
  , edition
  , users
  , trial
  , issued_date
  , start_date
  , expire_date
  , license_email
  , contact_sfid
  , account_sfid
  , account_name
  , stripeid
  , customer_id
  , number
  , MIN(license_activation_date) AS license_activation_date
  , MAX(timestamp)  AS last_active_date
FROM {{ ref('licenses') }}
WHERE server_id IS NOT NULL
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17
)

SELECT *
       , ROW_NUMBER() OVER (PARTITION BY server_id ORDER BY trial DESC, license_activation_date desc) AS license_priority_rank
       , CASE WHEN COALESCE((lead(start_date, 3) OVER (PARTITION BY server_id, trial ORDER BY issued_date))::DATE, expire_date::DATE + INTERVAL '1 DAY') <= expire_date 
            THEN (lead(start_date, 3) OVER (PARTITION BY server_id, trial ORDER BY issued_date))::DATE - interval '1 day'
          WHEN COALESCE((lead(start_date, 2) OVER (PARTITION BY server_id, trial ORDER BY issued_date))::DATE, expire_date::DATE + INTERVAL '1 DAY') <= expire_date 
            THEN (lead(start_date, 2) OVER (PARTITION BY server_id, trial ORDER BY issued_date))::DATE - interval '1 day'
          WHEN COALESCE((lead(start_date, 1) OVER (PARTITION BY server_id, trial ORDER BY issued_date))::DATE, expire_date::DATE + INTERVAL '1 DAY') <= expire_date
            THEN (lead(start_date, 1) OVER (PARTITION BY server_id, trial ORDER BY issued_date))::DATE - interval '1 day'
          ELSE expire_date END as license_retired_date
FROM license_server_fact
{% if is_incremental() %}

WHERE last_active_date >= (SELECT MAX(last_active_date) FROM {{this}})

{% endif %}