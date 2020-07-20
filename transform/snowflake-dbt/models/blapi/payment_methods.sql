{{config({
    "materialized": "table",
    "schema": "blapi"
  })
}}

WITH payment_methods AS (
    SELECT 
        payment_methods.id,
        payment_methods.state,
        payment_methods.version_id,
        payment_methods.previous_version_id,
        payment_methods.created_at,
        payment_methods.updated_at,
        payment_methods.deleted_at,
        payment_methods.payment_type,
        payment_methods.customer_id,
        payment_methods.address_id
    FROM {{ source('blapi','payment_methods') }}
)

SELECT * FROM payment_methods

