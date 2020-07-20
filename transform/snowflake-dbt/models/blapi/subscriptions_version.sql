{{config({
    "materialized": "table",
    "schema": "blapi"
  })
}}

WITH subscriptions_version AS (
    SELECT 
        subscriptions.version_id = subscriptions_version.version_id AS current_version,
        subscriptions_version.version_id,
        subscriptions_version.previous_version_id,
        subscriptions_version.created_at,
        subscriptions_version.updated_at,
        subscriptions_version.deleted_at,
        subscriptions_version.state,
        subscriptions_version.id,
        subscriptions_version.product_id,
        subscriptions_version.customer_id,
        subscriptions_version.start_date,
        subscriptions_version.end_date,
        subscriptions_version.num_seats,
        subscriptions_version.total_in_cents,
        subscriptions_version.transaction_id,
        subscriptions_version.end_transaction_id,
        subscriptions_version.operation_type,
        subscriptions_version.version_id_mod,
        subscriptions_version.previous_version_id_mod,
        subscriptions_version.created_at_mod,
        subscriptions_version.updated_at_mod,
        subscriptions_version.deleted_at_mod,
        subscriptions_version.state_mod,
        subscriptions_version.product_id_mod,
        subscriptions_version.customer_id_mod,
        subscriptions_version.start_date_mod,
        subscriptions_version.end_date_mod,
        subscriptions_version.num_seats_mod,
        subscriptions_version.total_in_cents_mod,
        subscriptions_version.license_issued_at,
        subscriptions_version.license_issued_at_mod,
        subscriptions_version.license_payload,
        subscriptions_version.license_payload_mod,
        subscriptions_version.sfdc_synced_at,
        subscriptions_version.sfdc_synced_at_mod,
        subscriptions_version.previous_subscription_version_id,
        subscriptions_version.previous_subscription_version_id_mod,
        subscriptions_version.subscription_version_id,
        subscriptions_version.subscription_version_id_mod
    FROM {{ source('blapi','subscriptions') }}
    LEFT JOIN {{ source('blapi','subscriptions_version') }} ON subscriptions.id = subscriptions_version.id
)

SELECT * FROM subscriptions_version

