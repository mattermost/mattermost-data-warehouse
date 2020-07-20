{{config({
    "materialized": "table",
    "schema": "blapi"
  })
}}

WITH addresses_version AS (
    SELECT 
        addresses.version_id = addresses_version.version_id AS current_version,
        addresses_version.version_id,
        addresses_version.previous_version_id,
        addresses_version.created_at,
        addresses_version.updated_at,
        addresses_version.deleted_at,
        addresses_version.id,
        addresses_version.line1,
        addresses_version.line2,
        addresses_version.city,
        addresses_version.state,
        addresses_version.country,
        addresses_version.postal_code,
        addresses_version.customer_id,
        addresses_version.address_type,
        addresses_version.transaction_id,
        addresses_version.end_transaction_id,
        addresses_version.operation_type,
        addresses_version.version_id_mod,
        addresses_version.previous_version_id_mod,
        addresses_version.created_at_mod,
        addresses_version.updated_at_mod,
        addresses_version.deleted_at_mod,
        addresses_version.line1_mod,
        addresses_version.line2_mod,
        addresses_version.city_mod,
        addresses_version.state_mod,
        addresses_version.country_mod,
        addresses_version.postal_code_mod,
        addresses_version.customer_id_mod,
        addresses_version.address_type_mod
    FROM {{ source('blapi','addresses') }}
    LEFT JOIN {{ source('blapi','addresses_version') }} ON addresses.id = addresses_version.id
)

SELECT * FROM addresses_version
