{{config({
    "materialized": "table",
    "schema": "blapi"
  })
}}

WITH products_version AS (
    SELECT 
        products.version_id = products_version.version_id AS current_version,
        products_version.version_id,
        products_version.previous_version_id,
        products_version.created_at,
        products_version.updated_at,
        products_version.deleted_at,
        products_version.state,
        products_version.id,
        products_version.name,
        products_version.description,
        products_version.sku,
        products_version.active,
        products_version.cents_per_seat,
        products_version.term_in_days,
        products_version.transaction_id,
        products_version.end_transaction_id,
        products_version.operation_type,
        products_version.version_id_mod,
        products_version.previous_version_id_mod,
        products_version.created_at_mod,
        products_version.updated_at_mod,
        products_version.deleted_at_mod,
        products_version.state_mod,
        products_version.name_mod,
        products_version.description_mod,
        products_version.sku_mod,
        products_version.active_mod,
        products_version.cents_per_seat_mod,
        products_version.term_in_days_mod,
        products_version.pricebookentryid,
        products_version.pricebookentryid_mod
    FROM {{ source('blapi','products') }}
    LEFT JOIN {{ source('blapi','products_version') }} ON products.id = products_version.id
)

SELECT * FROM products_version