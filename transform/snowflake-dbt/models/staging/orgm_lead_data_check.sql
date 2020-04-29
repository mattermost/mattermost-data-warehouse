{{config({
    "materialized": 'table',
    "schema": "staging",
    "post-hook": "{{ pg_import('staging.orgm_lead_data_check', 'pg_dummy_update') }}"
  })
}}

WITH orgm_lead_data_check AS (
    SELECT 
        sfid,
        systemmodstamp,
        createddate,
        ownerid,
        status,
        email,
        converteddate,
        TO_TIMESTAMP_NTZ(current_timestamp) AS processed_at
    FROM {{ source('orgm','lead') }}
)
SELECT * FROM orgm_lead_data_check