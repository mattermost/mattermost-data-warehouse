{{config({
    "materialized": "table",
    "schema": "blapi"
  })
}}

WITH payments_version AS (
    SELECT 
        payments.version_id = payments_version.version_id AS current_version,
        payments_version.version_id,
        payments_version.previous_version_id,
        payments_version.created_at,
        payments_version.updated_at,
        payments_version.deleted_at,
        payments_version.state,
        payments_version.id,
        payments_version.customer_id,
        payments_version.subscription_id,
        payments_version.payment_method_id,
        payments_version.total_in_cents,
        payments_version.stripe_status,
        payments_version.stripe_id,
        payments_version.transaction_id,
        payments_version.end_transaction_id,
        payments_version.operation_type,
        payments_version.version_id_mod,
        payments_version.previous_version_id_mod,
        payments_version.created_at_mod,
        payments_version.updated_at_mod,
        payments_version.deleted_at_mod,
        payments_version.state_mod,
        payments_version.customer_id_mod,
        payments_version.subscription_id_mod,
        payments_version.payment_method_id_mod,
        payments_version.total_in_cents_mod,
        payments_version.stripe_status_mod,
        payments_version.stripe_id_mod,
        payments_version.invoice_number,
        payments_version.invoice_number_mod,
        payments_version.stripe_charge_id,
        payments_version.stripe_charge_id_mod
    FROM {{ source('blapi','payments') }}
    LEFT JOIN {{ source('blapi','payments_version') }} ON payments.id = payments_version.id
)

SELECT * FROM payments_version
