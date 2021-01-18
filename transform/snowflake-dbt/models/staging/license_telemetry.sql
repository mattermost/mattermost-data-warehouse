{{config({
    "materialized": 'table',
    "schema": "staging",
    "post-hook": "{{ pg_import('staging.license_telemetry', 'update_onboarding_telemetry') }}"
  })
}}

WITH license_telemetry AS (
    SELECT
        licenseid,
        MAX(enterprise_license_fact.last_license_telemetry_date) AS last_telemetry_date,
        SUM(COALESCE(enterprise_license_fact.current_max_license_server_dau,0)) AS dau,
        SUM(COALESCE(enterprise_license_fact.current_max_license_server_mau,0)) AS mau,
        MAX(enterprise_license_fact.current_license_server_version) AS server_version,
        SUM(COALESCE(current_max_license_registered_users,0)) AS registered_users
    FROM blp.enterprise_license_fact
    JOIN orgm.opportunity on opportunity.license_key__c = enterprise_license_fact.licenseid
    WHERE enterprise_license_fact.last_license_telemetry_date IS NOT NULL
    GROUP BY 1
)

SELECT * FROM license_telemetry