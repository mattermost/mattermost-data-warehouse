{{config({
    "materialized": "table",
    "schema": "mattermost"
  })
}}

WITH contributor_employee_map_data AS (
    SELECT 
        'Employee' AS reason, 
        major_city,
        region_country
        email as unique_identifier
    FROM {{ source('employee', 'staff_list') }}
    UNION ALL
    SELECT 
        reason, 
        city,
        region_country,
        name
    FROM {{ source('staging', 'contributor_map_data') }}
)
SELECT * FROM contributor_employee_map_data