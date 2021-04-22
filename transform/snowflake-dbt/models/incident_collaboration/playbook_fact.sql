{{config({
    "materialized": "incremental",
    "schema": "incident_collaboration",
    "tags":"hourly",
    "unique_key":"id"
  })
}}

WITH 
{% if is_incremental() %}
playbook_updates AS (
  SELECT 
    COALESCE(ire.playbook_id, ire.playbookid) AS playbook
  FROM {{this}} pf
  JOIN {{ref('incident_response_events')}} ire
    ON pf.playbook_id = COALESCE(ire.playbook_id, ire.playbookid)
       AND pf.server_id = COALESCE(ire.user_id, ire.anonymous_id)
       AND ire.timestamp > pf.last_active_date
       AND ire.timestamp <= CURRENT_TIMESTAMP
       AND event = 'playbook'
  GROUP BY 1
), 

playbook_facts AS (
    SELECT 
          COALESCE(user_id, anonymous_id) AS server_id
        , COALESCE(playbook_id, playbookid) AS playbook_id
        , MIN(timestamp) AS created_date
        , MAX(CASE WHEN action = 'update' THEN timestamp ELSE NULL END) AS last_edit_date
        , MAX(CASE WHEN action = 'delete' THEN timestamp ELSE NULL END) AS deleted_date
        , MAX(timestamp) AS last_active_date
        , COUNT(DISTINCT COALESCE(plugin_version, pluginversion)) AS plugin_versions
        , COUNT(DISTINCT COALESCE(server_version, serverversion)) AS server_versions
        , COUNT(CASE WHEN action = 'create' THEN COALESCE(playbook_id, playbookid) ELSE NULL END) AS playbooks_created
        , COUNT(CASE WHEN action = 'update' THEN COALESCE(playbook_id, playbookid) ELSE NULL END) AS playbooks_edited
        , COUNT(CASE WHEN action = 'delete' THEN COALESCE(playbook_id, playbookid) ELSE NULL END) AS playbooks_deleted
        , COUNT(DISTINCT COALESCE(user_actual_id, useractualid)) AS playbook_users
        , {{ dbt_utils.surrogate_key(['COALESCE(user_id, anonymous_id)', 'COALESCE(playbook_id, playbookid)'])}} AS id
    FROM {{ref('incident_response_events')}}
    JOIN playbook_updates pu
      ON COALESCE(incident_response_events.playbook_id, incident_response_events.playbookid) = pu.playbook
    WHERE event = 'playbook'
    AND timestamp <= CURRENT_TIMESTAMP
    GROUP BY 1, 2

    UNION ALL

    SELECT 
          COALESCE(user_id, anonymous_id) AS server_id
        , COALESCE(playbook_id, playbookid) AS playbook_id
        , MIN(timestamp) AS created_date
        , MAX(CASE WHEN action = 'update' THEN timestamp ELSE NULL END) AS last_edit_date
        , MAX(CASE WHEN action = 'delete' THEN timestamp ELSE NULL END) AS deleted_date
        , MAX(timestamp) AS last_active_date
        , COUNT(DISTINCT COALESCE(plugin_version, pluginversion)) AS plugin_versions
        , COUNT(DISTINCT COALESCE(server_version, serverversion)) AS server_versions
        , COUNT(CASE WHEN action = 'create' THEN COALESCE(playbook_id, playbookid) ELSE NULL END) AS playbooks_created
        , COUNT(CASE WHEN action = 'update' THEN COALESCE(playbook_id, playbookid) ELSE NULL END) AS playbooks_edited
        , COUNT(CASE WHEN action = 'delete' THEN COALESCE(playbook_id, playbookid) ELSE NULL END) AS playbooks_deleted
        , COUNT(DISTINCT COALESCE(user_actual_id, useractualid)) AS playbook_users
        , {{ dbt_utils.surrogate_key(['COALESCE(user_id, anonymous_id)', 'COALESCE(playbook_id, playbookid)'])}} AS id
    FROM {{ref('incident_response_events')}}
    WHERE NOT EXISTS (SELECT playbook_id 
                      FROM {{this}} ir 
                      WHERE ir.playbook_id = COALESCE(incident_response_events.playbook_id, incident_response_events.playbookid))
    AND event = 'playbook'
    AND timestamp <= CURRENT_TIMESTAMP
    GROUP BY 1, 2 
),

playbook_fact AS (
    SELECT 
        pf.server_id
      , pf.playbook_id
      , pf.id
      , pf.created_date
      , pf.last_edit_date
      , pf.deleted_date
      , pf.last_active_date
      , COALESCE(ire.plugin_version, ire.pluginversion) AS plugin_version
      , COALESCE(ire.server_version, ire.serverversion) AS server_version
      , pf.plugin_versions
      , pf.server_versions
      , pf.playbooks_created
      , pf.playbooks_edited
      , pf.playbooks_deleted
      , pf.playbook_users
      , MIN(if.created_date) AS first_incident_start_date
      , MAX(if.created_date) AS last_incident_start_date
      , MAX(if.last_activity_date) AS last_incident_activity_date
      , COUNT(DISTINCT if.incident_id) AS incidents
      , COUNT(DISTINCT CASE WHEN if.reported THEN if.incident_id ELSE NULL END) AS reported_incidents
      , COUNT(DISTINCT CASE WHEN if.acknowledged THEN if.incident_id ELSE NULL END) AS acknowledged_incidents
      , COUNT(DISTINCT CASE WHEN if.resolved THEN if.incident_id ELSE NULL END) AS resolved_incidents
      , COUNT(DISTINCT CASE WHEN if.archived THEN if.incident_id ELSE NULL END) AS archived_incidents
      , MAX(if.server_version) AS latest_incident_server_version
      , MAX(if.plugin_version) AS latest_incident_plugin_version
      , MAX(if.incident_users) AS max_incident_users
    FROM playbook_facts pf
    JOIN {{ref('incident_response_events')}} ire
       ON pf.playbook_id = COALESCE(ire.playbook_id, ire.playbookid)
       AND pf.server_id = COALESCE(ire.user_id, ire.anonymous_id)
       AND pf.created_date = ire.timestamp
       AND ire.event = 'playbook'
    LEFT JOIN {{ ref('incident_fact' )}} if
        ON pf.server_id = if.server_id
        AND pf.playbook_id = if.playbook_id
    {{ dbt_utils.group_by(n=15)}}
)
{% else %}
playbook_facts AS (
    SELECT 
          COALESCE(user_id, anonymous_id) AS server_id
        , COALESCE(playbook_id, playbookid) AS playbook_id
        , MIN(timestamp) AS created_date
        , MAX(CASE WHEN action = 'update' THEN timestamp ELSE NULL END) AS last_edit_date
        , MAX(CASE WHEN action = 'delete' THEN timestamp ELSE NULL END) AS deleted_date
        , MAX(timestamp) AS last_active_date
        , COUNT(DISTINCT COALESCE(plugin_version, pluginversion)) AS plugin_versions
        , COUNT(DISTINCT COALESCE(server_version, serverversion)) AS server_versions
        , COUNT(CASE WHEN action = 'create' THEN COALESCE(playbook_id, playbookid) ELSE NULL END) AS playbooks_created
        , COUNT(CASE WHEN action = 'update' THEN COALESCE(playbook_id, playbookid) ELSE NULL END) AS playbooks_edited
        , COUNT(CASE WHEN action = 'delete' THEN COALESCE(playbook_id, playbookid) ELSE NULL END) AS playbooks_deleted
        , COUNT(DISTINCT COALESCE(user_actual_id, useractualid)) AS playbook_users
        , {{ dbt_utils.surrogate_key(['COALESCE(user_id, anonymous_id)', 'COALESCE(playbook_id, playbookid)'])}} AS id
    FROM {{ref('incident_response_events')}}
    WHERE event = 'playbook'
    AND timestamp <= CURRENT_TIMESTAMP
    GROUP BY 1, 2
),

playbook_fact AS (
    SELECT 
        pf.server_id
      , pf.playbook_id
      , pf.id
      , pf.created_date
      , pf.last_edit_date
      , pf.deleted_date
      , pf.last_active_date
      , COALESCE(ire.plugin_version, ire.pluginversion) AS plugin_version
      , COALESCE(ire.server_version, ire.serverversion) AS server_version
      , pf.plugin_versions
      , pf.server_versions
      , pf.playbooks_created
      , pf.playbooks_edited
      , pf.playbooks_deleted
      , pf.playbook_users
      , MIN(if.created_date) AS first_incident_start_date
      , MAX(if.created_date) AS last_incident_start_date
      , MAX(if.last_activity_date) AS last_incident_activity_date
      , COUNT(DISTINCT if.incident_id) AS incidents
      , COUNT(DISTINCT CASE WHEN if.reported THEN if.incident_id ELSE NULL END) AS reported_incidents
      , COUNT(DISTINCT CASE WHEN if.acknowledged THEN if.incident_id ELSE NULL END) AS acknowledged_incidents
      , COUNT(DISTINCT CASE WHEN if.resolved THEN if.incident_id ELSE NULL END) AS resolved_incidents
      , COUNT(DISTINCT CASE WHEN if.archived THEN if.incident_id ELSE NULL END) AS archived_incidents
      , MAX(if.server_version) AS latest_incident_server_version
      , MAX(if.plugin_version) AS latest_incident_plugin_version
      , MAX(if.incident_users) AS max_incident_users
    FROM playbook_facts pf
    JOIN {{ref('incident_response_events')}} ire
       ON pf.playbook_id = COALESCE(ire.playbook_id, ire.playbookid)
       AND pf.server_id = COALESCE(ire.user_id, ire.anonymous_id)
       AND pf.created_date = ire.timestamp
       AND ire.event = 'playbook'
    LEFT JOIN {{ ref('incident_fact' )}} if
        ON pf.server_id = if.server_id
        AND pf.playbook_id = if.playbook_id
    {{ dbt_utils.group_by(n=15)}}

)
{% endif %}

SELECT *
FROM playbook_fact

