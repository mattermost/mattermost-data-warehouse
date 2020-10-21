{{config({
    "materialized": "incremental",
    "schema": "blapi",
    "unique_key":"id",
    "tags":["hourly","blapi"]
  })
}}

WITH features AS (
    SELECT *
    FROM {{ source('blapi', 'features') }}
    {% if is_incremental() %}

    WHERE updated_at > (SELECT MAX(updated_at) FROM {{this}})

    {% endif %}
)

SELECT * 
FROM features