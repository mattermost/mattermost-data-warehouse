{{config({
    "materialized": "incremental",
    "schema": "incident_collaboration",
    "tags":"hourly",
    "unique_key":"id",
    "warehouse":"ANALYST_XS"
  })
}}
 
WITH min_dates AS (
    SELECT
        COALESCE(user_id, anonymous_id)         AS server_id
      , COALESCE(plugin_version, pluginversion) AS plugin_version
      , MIN(timestamp::DATE)                    AS first_version_date
      , MAX(timestamp) AS last_active
    FROM {{ ref('incident_response_events') }}
    WHERE timestamp::DATE <= CURRENT_TIMESTAMP
    GROUP BY 1, 2
                      ),
  
  version_dates AS (
      SELECT *
           , COALESCE(
                  LAG(first_version_date) OVER (PARTITION BY server_id ORDER BY first_version_date DESC) -
                  INTERVAL '1 day',
                  CURRENT_DATE)                             AS last_version_date
      FROM min_dates
     ),
     
  dates AS (
      SELECT
          d.date
        , vd.server_id
        , vd.plugin_version
        , vd.first_version_date
        , vd.last_version_date
      FROM {{ source('util', 'dates') }} d 
      JOIN version_dates vd
        ON d.date >= vd.first_version_date
        AND d.date <= vd.last_version_date
      {% if is_incremental() %}
      WHERE last_active > (SELECT MAX(last_active) FROM {{this}})
      {% endif %}
     ),

incident_daily_details AS (
    SELECT
        d.date
      , {{ dbt_utils.surrogate_key(['d.date',
                 'd.server_id', 'd.plugin_version'])}}                                                AS id
      , d.server_id
      , d.plugin_version
      , d.first_version_date
      , d.last_version_date
      , MIN(events.timestamp)                                                                         AS first_active
      , MAX(events.timestamp)                                                                         AS last_active
      , count(distinct COALESCE(events.playbook_id, events.playbookid))                               AS playbooks
      , COUNT(DISTINCT CASE WHEN event = 'playbook' and action = 'create' 
                            THEN COALESCE(events.playbook_id, events.playbookid)
                            ELSE NULL END)                                                            AS playbooks_created
      , COUNT(DISTINCT CASE WHEN event = 'playbook' and action = 'update' 
                            THEN COALESCE(events.playbook_id, events.playbookid)
                            ELSE NULL END)                                                            AS playbooks_edited
      , COUNT(DISTINCT CASE WHEN event = 'playbook' and action = 'deleted' 
                            THEN COALESCE(events.playbook_id, events.playbookid)
                            ELSE NULL END)                                                            AS playbooks_deleted
      , COUNT(DISTINCT CASE WHEN currentstatus = 'Reported' THEN events.incident_id ELSE NULL END)    AS reported_incidents
      , COUNT(DISTINCT CASE WHEN currentstatus = 'Active' THEN events.incident_id ELSE NULL END)      AS acknowledged_incidents
      , COUNT(DISTINCT CASE WHEN currentstatus = 'Archived' THEN events.incident_id ELSE NULL END)    AS archived_incidents
      , COUNT(DISTINCT CASE WHEN currentstatus = 'Resolved' THEN events.incident_id ELSE NULL END)    AS resolved_incidents
      , COUNT(DISTINCT COALESCE(events.useractualid, events.user_actual_id))                          AS incident_contributors
      , COUNT(DISTINCT CASE
                  WHEN event = 'incident' AND action = 'update_status' THEN events.id
                                                                       ELSE NULL END)                 AS status_updates
      , COUNT(DISTINCT CASE
                  WHEN event = 'incident' AND action = 'change_stage' THEN events.id
                                                                      ELSE NULL END)                  AS stages_changed
      , COUNT(DISTINCT CASE
                  WHEN event = 'incident' AND action = 'add_timeline_event_from_post' THEN events.id
                                                                                      ELSE NULL END)  AS timeline_events_added
      , COUNT(DISTINCT CASE
                  WHEN event = 'incident' AND action = 'change_commander' THEN events.id
                                                                          ELSE NULL END)              AS commanders_changed
      , COUNT(DISTINCT CASE WHEN event = 'frontend' AND action = 'timeline_tab_clicked' 
                  THEN events.id ELSE NULL END)                                                       AS timeline_tab_clicks
      , COUNT(DISTINCT CASE WHEN event = 'incident' AND action = 'end' THEN events.id ELSE NULL END)  AS ended_incident
      , COUNT(
                DISTINCT CASE WHEN event = 'incident' AND action = 'restart' 
                THEN events.id ELSE NULL END)                                                         AS restarted_incident
      , COUNT(DISTINCT CASE
                  WHEN event = 'tasks' AND action = 'set_assignee_for_task' THEN events.id
                                                                            ELSE NULL END)            AS task_assignees_set
      , COUNT(DISTINCT CASE
                  WHEN event = 'tasks' AND action = 'modify_task_state' THEN events.id
                                                                        ELSE NULL END)                AS task_states_modified
      , COUNT(DISTINCT CASE WHEN event = 'tasks' AND action = 'add_task' 
                THEN events.id ELSE NULL END)                                                         AS tasks_added
      , COUNT(DISTINCT CASE WHEN event = 'tasks' AND action = 'remove_task' 
                THEN events.id ELSE NULL END)                                                         AS tasks_removed
      , COUNT(DISTINCT CASE WHEN event = 'tasks' AND action = 'rename_task' 
              THEN events.id ELSE NULL END)                                                           AS tasks_renamed
      , COUNT(DISTINCT CASE
                  WHEN event = 'tasks' AND action = 'run_task_slash_command' THEN events.id
                                                                             ELSE NULL END)           AS task_slash_commands_run
      , COUNT(CASE WHEN event = 'tasks' AND action = 'move_task' THEN events.id ELSE NULL END)          AS tasks_moved
    FROM dates d
    JOIN {{ ref('incident_response_events') }} events
      ON d.server_id = COALESCE(events.user_id, events.anonymous_id)
      AND events.timestamp::date <= d.date
      AND events.timestamp::date >= first_version_date
    WHERE events.timestamp::DATE <= CURRENT_TIMESTAMP
    GROUP BY 1, 2, 3, 4, 5, 6
                         )

SELECT *
FROM incident_daily_details