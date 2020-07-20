{{config({
    "materialized": "table",
    "schema": "blapi"
  })
}}

WITH product_addons_version AS (
    SELECT 
        product_addons.addon_id = product_addons_version.addon_id AND product_addons.product_id = product_addons_version.product_id AS current_version,
        product_addons_version.product_id,
        product_addons_version.addon_id,
        product_addons_version.transaction_id,
        product_addons_version.end_transaction_id,
        product_addons_version.operation_type
    FROM {{ source('blapi','product_addons') }}
    LEFT JOIN {{ source('blapi','product_addons_version') }} ON product_addons.addon_id = product_addons_version.addon_id 
        AND product_addons.product_id = product_addons_version.product_id
)

SELECT * FROM product_addons_version