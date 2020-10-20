{{config({
    "materialized": "incremental",
    "schema": "blp",
    "unique_key":"id",
    "tags":"nightly"
  })
}}

WITH features AS (
    SELECT *
    FROM {{ source('blapi', 'features') }}
    {% if is_incremental() %}

    WHERE created_at::date > (SELECT MAX(created_at::date) FROM {{this}})

    {% endif %}
)

SELECT * 
FROM features