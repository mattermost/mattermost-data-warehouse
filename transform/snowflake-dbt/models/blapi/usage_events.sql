{{config({
    "materialized": "incremental",
    "schema": "blp",
    "unique_key":"id",
    "tags":["nightly","blapi"],
    "database":"DEV"
  })
}}

WITH usage_events AS (
  SELECT *
  FROM {{ source('blapi', 'usage_events') }}
  {% if is_incremental() %}

  WHERE timestamp >= (SELECT MAX(timestamp) FROM {{this}})

  {% endif %}
)

SELECT *
FROM usage_events