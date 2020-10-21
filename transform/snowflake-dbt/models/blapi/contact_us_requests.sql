{{config({
    "materialized": "incremental",
    "schema": "blapi",
    "unique_key":"id",
    "tags":["hourly","blapi"]
  })
}}

WITH contact_us_requests AS (
    SELECT *
    FROM {{ source('blapi', 'contact_us_requests') }}
    {% if is_incremental() %}

    WHERE created_at >= (SELECT MAX(created_at) FROM {{this}})

    {% endif %}
)

SELECT *
FROM contact_us_requests