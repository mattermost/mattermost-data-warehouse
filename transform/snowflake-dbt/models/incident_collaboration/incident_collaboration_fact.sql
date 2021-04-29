{{config({
    "materialized": "incremental",
    "schema": "incident_collaboration",
    "tags":"hourly",
    "unique_key":"server_id",
    "warehouse":"ANALYST_XS"
  })
}}

WITH incident_collaboration_fact AS (
    SELECT 
        server_id
        , MIN(date) AS first_active_date
        , MAX(last_active) AS last_active_date
        , MAX(plugin_version) AS current_plugin_version
        , MAX(version_users_to_date) AS users_max
        , MAX(daily_active_users) AS daily_active_users_max
        , SUM(CASE WHEN date = last_version_date::date THEN playbooks_created ELSE 0 END) AS playbooks_created
        , SUM(CASE WHEN date = last_version_date::date THEN incidents_reported ELSE 0 END) AS incidents_reported
        , SUM(CASE WHEN date = last_version_date::date THEN incidents_acknowledged ELSE 0 END) AS incidents_acknowledged
        , SUM(CASE WHEN date = last_version_date::date THEN incidents_resolved ELSE 0 END) AS incidents_resolved
        , SUM(CASE WHEN date = last_version_date::date THEN incidents_archived ELSE 0 END) AS incidents_archived
    FROM {{ref('incident_daily_details')}}
    GROUP BY 1
    {% if is_incremental() %}
    HAVING MAX(last_active::date) >= (SELECT MAX(last_active_date::date) FROM {{this}})
    {% endif %}
)

SELECT *
FROM incident_collaboration_fact