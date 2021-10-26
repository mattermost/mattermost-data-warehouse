{{config({
    "materialized": "incremental",
    "schema": "incident_collaboration",
    "tags":"hourly",
    "unique_key":"server_id",
    "warehouse":"ANALYST_XS"
  })
}}

WITH min_date AS (
    SELECT 
        server_id
      , MIN(date) AS first_active_date
    FROM {{ref('incident_daily_details')}}
    GROUP BY 1
),


incident_collaboration_fact AS (
    SELECT 
        incident_daily_details.server_id
        , MIN(incident_daily_details.date) AS first_active_date
        , MAX(incident_daily_details.last_active) AS last_active_date
        , MAX(CASE WHEN CASE WHEN license_server_fact.edition IS NOT NULL AND NOT license_server_fact.trial THEN license_server_fact.edition
                      WHEN license_server_fact.edition = 'Mattermost Cloud' THEN 'Mattermost Cloud'
                      WHEN license_server_fact.edition IS NOT NULL AND license_server_fact.trial THEN 'E20 Trial'
                      WHEN license_server_fact.customer_id is not null and NOT COALESCE(license_server_fact.trial, TRUE) THEN 'E10'
                      ELSE COALESCE(server_daily_details.edition, server_fact.server_edition)
                      END = 'E20' THEN 5
                   WHEN CASE WHEN license_server_fact.edition IS NOT NULL AND NOT license_server_fact.trial THEN license_server_fact.edition
                      WHEN license_server_fact.edition = 'Mattermost Cloud' THEN 'Mattermost Cloud'
                      WHEN license_server_fact.edition IS NOT NULL AND license_server_fact.trial THEN 'E20 Trial'
                      WHEN license_server_fact.customer_id is not null and NOT COALESCE(license_server_fact.trial, TRUE) THEN 'E10'
                      ELSE COALESCE(server_daily_details.edition, server_fact.server_edition)
                      END = 'E10' THEN 4
                   WHEN CASE WHEN license_server_fact.edition IS NOT NULL AND NOT license_server_fact.trial THEN license_server_fact.edition
                      WHEN license_server_fact.edition = 'Mattermost Cloud' THEN 'Mattermost Cloud'
                      WHEN license_server_fact.edition IS NOT NULL AND license_server_fact.trial THEN 'E20 Trial'
                      WHEN license_server_fact.customer_id is not null and NOT COALESCE(license_server_fact.trial, TRUE) THEN 'E10'
                      ELSE COALESCE(server_daily_details.edition, server_fact.server_edition)
                      END = 'E20 Trial' THEN 3
                   WHEN CASE WHEN license_server_fact.edition IS NOT NULL AND NOT license_server_fact.trial THEN license_server_fact.edition
                      WHEN license_server_fact.edition = 'Mattermost Cloud' THEN 'Mattermost Cloud'
                      WHEN license_server_fact.edition IS NOT NULL AND license_server_fact.trial THEN 'E20 Trial'
                      WHEN license_server_fact.customer_id is not null and NOT COALESCE(license_server_fact.trial, TRUE) THEN 'E10'
                      ELSE COALESCE(server_daily_details.edition, server_fact.server_edition)
                      END = 'Mattermost Cloud' THEN 2
                   WHEN CASE WHEN license_server_fact.edition IS NOT NULL AND NOT license_server_fact.trial THEN license_server_fact.edition
                      WHEN license_server_fact.edition = 'Mattermost Cloud' THEN 'Mattermost Cloud'
                      WHEN license_server_fact.edition IS NOT NULL AND license_server_fact.trial THEN 'E20 Trial'
                      WHEN license_server_fact.customer_id is not null and NOT COALESCE(license_server_fact.trial, TRUE) THEN 'E10'
                      ELSE COALESCE(server_daily_details.edition, server_fact.server_edition)
                      END = 'E0' THEN 1
                   WHEN CASE WHEN license_server_fact.edition IS NOT NULL AND NOT license_server_fact.trial THEN license_server_fact.edition
                      WHEN license_server_fact.edition = 'Mattermost Cloud' THEN 'Mattermost Cloud'
                      WHEN license_server_fact.edition IS NOT NULL AND license_server_fact.trial THEN 'E20 Trial'
                      WHEN license_server_fact.customer_id is not null and NOT COALESCE(license_server_fact.trial, TRUE) THEN 'E10'
                      ELSE COALESCE(server_daily_details.edition, server_fact.server_edition)
                      END = 'TE' THEN 0
                   ELSE 0 END) AS first_product_edition
        , MAX(incident_daily_details.plugin_version) AS current_plugin_version
        , MAX(incident_daily_details.version_users_to_date) AS users_max
        , MAX(incident_daily_details.daily_active_users) AS daily_active_users_max
        , MAX(incident_daily_details.weekly_active_users) AS weekly_active_users_max
        , MAX(incident_daily_details.monthly_active_users) AS monthly_active_users_max
        , SUM(CASE WHEN incident_daily_details.date = incident_daily_details.last_version_date::date 
                    THEN incident_daily_details.playbooks_created_alltime ELSE 0 END) AS playbooks_created
        , SUM(CASE WHEN incident_daily_details.date = incident_daily_details.last_version_date::date 
                    THEN incident_daily_details.playbooks_edited_alltime ELSE 0 END) AS playbooks_edited
        , SUM(CASE WHEN incident_daily_details.date = incident_daily_details.last_version_date::date 
                    THEN incident_daily_details.reported_incidents_alltime ELSE 0 END) AS incidents_reported
        , SUM(CASE WHEN incident_daily_details.date = incident_daily_details.last_version_date::date 
                    THEN acknowledged_incidents_alltime ELSE 0 END) AS incidents_acknowledged
        , SUM(CASE WHEN incident_daily_details.date = incident_daily_details.last_version_date::date 
                    THEN incident_daily_details.resolved_incidents_alltime ELSE 0 END) AS incidents_resolved
        , SUM(CASE WHEN incident_daily_details.date = incident_daily_details.last_version_date::date 
                    THEN incident_daily_details.archived_incidents_alltime ELSE 0 END) AS incidents_archived
        , COUNT(DISTINCT CASE WHEN incident_daily_details.daily_active_users > 0 
                    THEN incident_daily_details.date else null end) AS days_active
    FROM {{ref('incident_daily_details')}}
    JOIN min_date md
        ON incident_daily_details.server_id = md.server_id
    LEFT JOIN blp.license_server_fact  AS license_server_fact 
        ON (license_server_fact.server_id = md.server_id) and ((TO_CHAR(TO_DATE(md.first_active_date ), 'YYYY-MM-DD')) BETWEEN (TO_CHAR(TO_DATE(license_server_fact.start_date ), 'YYYY-MM-DD')) AND (TO_CHAR(TO_DATE(license_server_fact.license_retired_date ), 'YYYY-MM-DD')))
    LEFT JOIN mattermost.server_fact  AS server_fact 
        ON server_fact.server_id = incident_daily_details.server_id
    LEFT JOIN mattermost.server_daily_details  AS server_daily_details 
        ON incident_daily_details.server_id = server_daily_details.server_id AND (TO_CHAR(TO_DATE(server_daily_details.date ), 'YYYY-MM-DD'))::DATE = (TO_CHAR(TO_DATE(md.first_active_date ), 'YYYY-MM-DD'))::DATE
    GROUP BY 1
    {% if is_incremental() %}
    HAVING MAX(last_active::date) >= (SELECT MAX(last_active_date::date) FROM {{this}})
    {% endif %}
)

SELECT *
FROM incident_collaboration_fact