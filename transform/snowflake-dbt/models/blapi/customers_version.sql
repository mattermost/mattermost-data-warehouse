{{config({
    "materialized": "table",
    "schema": "blapi"
  })
}}

WITH customers_version AS (
    SELECT 
        customers.version_id = customers_version.version_id AS current_version,
        customers_version.version_id,
        customers_version.previous_version_id,
        customers_version.created_at,
        customers_version.updated_at,
        customers_version.deleted_at,
        customers_version.state,
        customers_version.id,
        customers_version.email,
        customers_version.stripe_id,
        customers_version.company_name,
        customers_version.transaction_id,
        customers_version.end_transaction_id,
        customers_version.operation_type,
        customers_version.version_id_mod,
        customers_version.previous_version_id_mod,
        customers_version.created_at_mod,
        customers_version.updated_at_mod,
        customers_version.deleted_at_mod,
        customers_version.state_mod,
        customers_version.email_mod,
        customers_version.stripe_id_mod,
        customers_version.company_name_mod,
        customers_version.sfdc_synced_at,
        customers_version.sfdc_synced_at_mod,
        customers_version.first_name,
        customers_version.first_name_mod,
        customers_version.last_name,
        customers_version.last_name_mod
    FROM {{ source('blapi','customers') }}
    LEFT JOIN {{ source('blapi','customers_version') }} ON customers.id = customers_version.id
)

SELECT * FROM customers_version