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
        , MAX(weekly_active_users) AS weekly_active_users_max
        , MAX(monthly_active_users) AS monthly_active_users_max
        , SUM(CASE WHEN date = last_version_date::date THEN playbooks_created ELSE 0 END) AS playbooks_created
        , SUM(CASE WHEN date = last_version_date::date THEN playbooks_edited ELSE 0 END) AS playbooks_edited
        , SUM(CASE WHEN date = last_version_date::date THEN reported_incidents ELSE 0 END) AS incidents_reported
        , SUM(CASE WHEN date = last_version_date::date THEN acknowledged_incidents ELSE 0 END) AS incidents_acknowledged
        , SUM(CASE WHEN date = last_version_date::date THEN resolved_incidents ELSE 0 END) AS incidents_resolved
        , SUM(CASE WHEN date = last_version_date::date THEN archived_incidents ELSE 0 END) AS incidents_archived
        , COUNT(DISTINCT CASE WHEN daily_active_users > 0 THEN date else null end) AS days_active
    FROM {{ref('incident_daily_details')}}
    GROUP BY 1
    {% if is_incremental() %}
    HAVING MAX(last_active::date) >= (SELECT MAX(last_active_date::date) FROM {{this}})
    {% endif %}
)

SELECT *
FROM incident_collaboration_fact