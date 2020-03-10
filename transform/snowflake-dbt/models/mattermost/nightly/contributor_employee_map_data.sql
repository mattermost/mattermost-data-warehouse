{{config({
    "materialized": "table",
    "schema": "mattermost"
  })
}}

WITH staff_list AS (
        SELECT
            CASE
                WHEN major_city IN ('New York City','New Jersey') THEN 'New York'
                WHEN major_city = 'Bangalore' THEN 'Bengalūru'
                WHEN major_city = 'Lake Tahoe' THEN 'Reno'
                WHEN major_city = 'Waterloo' OR major_city = 'Toronto/Waterloo' THEN 'Toronto'
                WHEN major_city = 'Luzon' THEN 'Baguio City'
                WHEN major_city = 'North District' THEN 'Nazareth'
                ELSE major_city
            END as major_city,
            CASE
                WHEN region_country = 'USA' THEN 'United States'
                WHEN region_country = 'UK' THEN 'United Kingdom'
                WHEN region_country = 'South Korea' THEN 'Korea, South'
                ELSE region_country
            END AS region_country,
            email
        FROM {{ source('employee', 'staff_list') }}
), contributor_employee_map_data AS (
    SELECT 
        'Employee' AS reason, 
        major_city,
        region_country,
        email as email,
        1 as count,
        lat,
        lng
    FROM staff_list
    LEFT JOIN {{ source('util', 'world_cities') }} ON staff_list.major_city = world_cities.city AND staff_list.region_country = world_cities.country
    UNION ALL
    SELECT
        reason,
        city,
        region_country,
        null,
        count,
        latitude,
        longitude
    FROM {{ source('staging', 'contributor_map_data') }}
)
SELECT * FROM contributor_employee_map_data