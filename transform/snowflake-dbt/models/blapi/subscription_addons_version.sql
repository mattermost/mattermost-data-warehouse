{{config({
    "materialized": "table",
    "schema": "blapi"
  })
}}

WITH subscription_addons_version AS (
    SELECT
        subscription_addons_version.subscription_id,
        subscription_addons_version.addon_id,
        subscription_addons_version.transaction_id,
        subscription_addons_version.end_transaction_id,
        subscription_addons_version.operation_type
    FROM {{ source('blapi','subscription_addons') }}
    LEFT JOIN {{ source('blapi','subscription_addons_version') }} ON subscription_addons.subscription_id = subscription_addons_version.subscription_id
        AND subscription_addons.addon_id = subscription_addons_version.addon_id
)

SELECT * FROM subscription_addons_version