{{config({
    "materialized": "incremental",
    "schema": "blapi",
    "unique_key":"id",
    "alias":"customers",
    "tags":["hourly","blapi", "deprecated"]
  })
}}

WITH customers AS (
    SELECT *
    FROM {{ source('blapi', 'customers') }}
    {% if is_incremental() %}

    WHERE updated_at >= (SELECT MAX(updated_at) FROM {{this}})

    {% endif %}
)

SELECT *
FROM customers