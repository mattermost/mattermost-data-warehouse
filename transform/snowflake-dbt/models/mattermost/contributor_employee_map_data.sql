{{config({
    "materialized": "table",
    "schema": "mattermost"
  })
}}

WITH contributor_employee_map_data AS (
    SELECT 
        'employee' AS reason, 
        major_city,
        email as unique_identifier
    FROM {{ source('employee', 'staff_list') }}
    UNION ALL
    SELECT 
        reason, 
        city,
        name
    FROM {{ source('staging', 'contributor_map_data') }}
)
SELECT * FROM contributor_employee_map_data