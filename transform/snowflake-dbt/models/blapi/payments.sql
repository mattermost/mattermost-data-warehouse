{{config({
    "materialized": "incremental",
    "schema": "blapi",
    "unique_key":"id",
    "tags": ["hourly", "blapi", "deprecated"]
  })
}}

WITH payments AS (
    SELECT *
    FROM {{ source('blapi', 'payments') }}
    {% if is_incremental() %}

    WHERE created_at >= (SELECT MAX(created_at) FROM {{this}})

    {% endif %}
)

SELECT * 
FROM payments