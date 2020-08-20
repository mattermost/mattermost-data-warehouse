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
  , MIN(license_activation_date) AS license_activation_date
  , MAX(timestamp)  AS last_active_date
FROM {{ ref('licenses') }}
WHERE server_id IS NOT NULL
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
)

SELECT *
FROM license_server_fact
{% if is_incremental() %}

WHERE last_active_date >= (SELECT MAX(last_active_date) FROM {{this}})

{% endif %}