{{config({
    "materialized": "incremental",
    "schema": "blp",
    "unique_key":"id",
    "tags":"nightly"
  })
}}

WITH customers AS (
    SELECT *
    FROM {{ source('blapi', 'customers') }}
    {% if is_incremental() %}



    {% endif %}
)

SELECT *
FROM customers