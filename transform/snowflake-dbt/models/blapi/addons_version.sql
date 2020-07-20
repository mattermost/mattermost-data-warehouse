{{config({
    "materialized": "table",
    "schema": "blapi"
  })
}}

WITH addons_version AS (
    SELECT 
        addons.version_id = addons_version.version_id AS current_version,
        addons_version.version_id,
        addons_version.previous_version_id,
        addons_version.created_at,
        addons_version.updated_at,
        addons_version.deleted_at,
        addons_version.id,
        addons_version.name,
        addons_version.display_name,
        addons_version.cents_per_seat,
        addons_version.transaction_id,
        addons_version.end_transaction_id,
        addons_version.operation_type,
        addons_version.version_id_mod,
        addons_version.previous_version_id_mod,
        addons_version.created_at_mod,
        addons_version.updated_at_mod,
        addons_version.deleted_at_mod,
        addons_version.name_mod,
        addons_version.display_name_mod,
        addons_version.cents_per_seat_mod,
        addons_version.pricebookentryid,
        addons_version.pricebookentryid_mod
    FROM {{ source('blapi','addons') }}
    LEFT JOIN {{ source('blapi','addons_version') }} ON addons.id = addons_version.id
)

SELECT * FROM addons_version




