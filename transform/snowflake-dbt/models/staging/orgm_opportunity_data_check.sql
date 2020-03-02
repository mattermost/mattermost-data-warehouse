{{config({
    "materialized": 'table',
    "schema": "staging",
    "post-hook": "{{ pg_import('staging.orgm_opportunity_data_check', 'pg_dummy_update') }}"
  })
}}

WITH orgm_opportunity_data_check AS (
    SELECT 
        sfid,
        systemmodstamp,
        createddate,
        current_timestamp AS processed_at
    FROM {{ source('orgm','opportunity') }}
)
SELECT * FROM orgm_opportunity_data_check