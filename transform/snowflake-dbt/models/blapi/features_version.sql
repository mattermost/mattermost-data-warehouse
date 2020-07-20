{{config({
    "materialized": "table",
    "schema": "blapi"
  })
}}

WITH features_version AS (
    SELECT 
        features.version_id = features_version.version_id AS current_version,
        features_version.version_id,
        features_version.previous_version_id,
        features_version.created_at,
        features_version.updated_at,
        features_version.deleted_at,
        features_version.state,
        features_version.id,
        features_version.name,
        features_version.display_name,
        features_version.description,
        features_version.transaction_id,
        features_version.end_transaction_id,
        features_version.operation_type,
        features_version.version_id_mod,
        features_version.previous_version_id_mod,
        features_version.created_at_mod,
        features_version.updated_at_mod,
        features_version.deleted_at_mod,
        features_version.state_mod,
        features_version.name_mod,
        features_version.display_name_mod,
        features_version.description_mod
    FROM {{ source('blapi','features') }}
    LEFT JOIN {{ source('blapi','features_version') }} ON features.id = features_version.id
)

SELECT * FROM features_version
