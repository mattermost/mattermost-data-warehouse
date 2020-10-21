{{config({
    "materialized": "incremental",
    "schema": "blp",
    "unique_key":"id",
    "tags":["nightly","blapi"],
    "database":"DEV"
  })
}}

WITH payments AS (
    SELECT *
    FROM {{ source('blapi', 'payments') }}
    {% if is_incremental() %}

    WHERE created_at::date > (SELECT MAX(DATE) FROM {{this}})

    {% endif %}
)

SELECT * 
FROM payments