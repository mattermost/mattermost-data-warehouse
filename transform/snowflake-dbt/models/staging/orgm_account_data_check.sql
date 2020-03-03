{{config({
    "materialized": 'table',
    "schema": "staging",
    "post-hook": "{{ pg_import('staging.orgm_account_data_check', 'pg_dummy_update') }}"
  })
}}

WITH orgm_account_data_check AS (
    SELECT 
        sfid,
        systemmodstamp,
        createddate,
        TO_TIMESTAMP_NTZ(current_timestamp) AS processed_at
    FROM {{ source('orgm','account') }}
)
SELECT * FROM orgm_account_data_check