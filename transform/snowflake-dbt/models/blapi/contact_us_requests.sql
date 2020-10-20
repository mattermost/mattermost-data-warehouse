{{config({
    "materialized": "incremental",
    "schema": "blp",
    "unique_key":"id",
    "tags":"nightly"
  })
}}

WITH contact_us_requests AS (
    SELECT *
    FROM {{ source('blapi', 'contact_us_requests') }}
    {% if is_incremental() %}

    WHERE created_at::date >= (SELECT MAX(created_at::date) FROM {{this}})

    {% endif %}
)

SELECT *
FROM contact_us_requests