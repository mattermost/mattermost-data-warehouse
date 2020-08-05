{{config({
    "materialized": 'table',
    "schema": "staging",
    "post-hook": "{{ pg_import('staging.orgm_account_telemetry', 'update_account_telemetry') }}"
  })
}}

WITH orgm_account_telemetry AS (
    SELECT
        account_sfid,
        MAX(enterprise_license_fact.last_license_telemetry_date) AS last_telemetry_date,
        SUM(COALESCE(enterprise_license_fact.current_max_license_server_dau,0)) AS dau,
        SUM(COALESCE(enterprise_license_fact.current_max_license_server_mau,0)) AS mau
    FROM blp.enterprise_license_fact
    LEFT JOIN orgm.account ON account.sfid = enterprise_license_fact.account_sfid
    WHERE enterprise_license_fact.last_license_telemetry_date IS NOT NULL
    GROUP BY 1
)

SELECT * FROM orgm_account_telemetry