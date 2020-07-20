{{config({
    "materialized": "table",
    "schema": "blapi"
  })
}}

WITH product_features_version AS (
    SELECT 
        product_features.version_id = product_features_version.version_id AS current_version,
        product_features_version.version_id,
        product_features_version.previous_version_id,
        product_features_version.created_at,
        product_features_version.updated_at,
        product_features_version.deleted_at,
        product_features_version.id,
        product_features_version.feature_id,
        product_features_version.product_id,
        product_features_version.active,
        product_features_version.transaction_id,
        product_features_version.end_transaction_id,
        product_features_version.operation_type,
        product_features_version.version_id_mod,
        product_features_version.previous_version_id_mod,
        product_features_version.created_at_mod,
        product_features_version.updated_at_mod,
        product_features_version.deleted_at_mod,
        product_features_version.feature_id_mod,
        product_features_version.product_id_mod,
        product_features_version.active_mod
    FROM {{ source('blapi','product_features') }}
    LEFT JOIN {{ source('blapi','product_features_version') }} ON product_features.id = product_features_version.id
)

SELECT * FROM product_features_version