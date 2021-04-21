{{config({
    "materialized": "incremental",
    "schema": "incident_collaboration",
    "tags":"hourly",
    "unique_key":"id"
  })
}}

WITH 
{% if is_incremental() %}
min_user_date AS (
    SELECT 
          COALESCE(incident_response_events.user_id, incident_response_events.anonymous_id) as server_id
        , COALESCE(incident_response_events.incident_id, incident_response_events.incidentid) AS incident_id
        , COALESCE(incident_response_events.user_actual_id, incident_response_events.useractualid) AS user_id
        , MIN(timestamp) as first_activity_date
    FROM {{ ref('incident_response_events')}}
    JOIN {{this}} ic
        ON COALESCE(incident_response_events.user_id, incident_response_events.anonymous_id) = ic.server_id
        AND COALESCE(incident_response_events.incident_id, incident_response_events.incidentid) = ic.incident_id
        AND incident_response_events.timestamp > ic.last_activity_date
    WHERE incident_response_events.event in ('task', 'incident', 'frontend')
    AND incident_response_events.timestamp::DATE <= CURRENT_TIMESTAMP
    AND COALESCE(incident_response_events.incident_id, incident_response_events.incidentid) IS NOT NULL
    GROUP BY 1, 2, 3
),

incident_details AS (
        SELECT
        COALESCE(ic.id, {{ dbt_utils.surrogate_key(['COALESCE(events.incident_id, events.incidentid)',
                 'COALESCE(events.user_id, events.anonymous_id)'])}})                                               AS id
      , COALESCE(ic.incident_id, COALESCE(events.incident_id, events.incidentid))                                   AS incident_id
      , COALESCE(ic.server_id, COALESCE(events.user_id, events.anonymous_id))                                       AS server_id
      , COALESCE(MAX(ic.playbook_id), MAX(COALESCE(events.playbook_id, events.playbookid)))                          AS playbook_id
      , COALESCE(MIN(ic.created_date), MIN(events.timestamp))                                                        AS created_date
      , MAX(events.timestamp)                                                                         AS last_activity_date
      , COALESCE(MIN(ic.resolved_date), 
        MIN(CASE WHEN currentstatus = 'Resolved' THEN events.timestamp ELSE NULL END))                 AS resolved_date
      , COALESCE(MIN(ic.archived_date), 
        MIN(CASE WHEN currentstatus = 'Archived' THEN events.timestamp ELSE NULL END))                 AS archived_date
      , COALESCE(MAX(CASE WHEN currentstatus = 'Reported' THEN TRUE ELSE NULL END),
        MAX(ic.reported))                                                                             AS reported
      , COALESCE(MAX(CASE WHEN currentstatus = 'Active' THEN TRUE ELSE NULL END),
        MAX(ic.acknowledged))                                                                         AS acknowledged
      , COALESCE(MAX(CASE WHEN currentstatus = 'Resolved' THEN TRUE ELSE NULL END),
        MAX(ic.resolved))                                                                             AS resolved
      , COALESCE(MAX(CASE WHEN currentstatus = 'Archived' THEN TRUE ELSE NULL END),
        MAX(ic.archived))                                                                             AS archived
      , MAX(ic.incident_users) + COALESCE(COUNT(DISTINCT mud.user_id), 0)                             AS incident_users
      , MAX(ic.status_updates) + COALESCE(COUNT(CASE
                  WHEN event = 'incident' AND action = 'update_status' THEN events.id
                                                                       ELSE NULL END), 0)                AS status_updates
      , MAX(ic.stages_changed) + COALESCE(COUNT(CASE
                  WHEN event = 'incident' AND action = 'change_stage' THEN events.id
                                                                      ELSE NULL END), 0)                 AS stages_changed
      , MAX(ic.timeline_events_added) + COALESCE(COUNT(CASE
                  WHEN event = 'incident' AND action = 'add_timeline_event_from_post' THEN events.id
                                                                                      ELSE NULL END),0)  AS timeline_events_added
      , MAX(ic.commanders_changed) + COALESCE(COUNT(CASE
                  WHEN event = 'incident' AND action = 'change_commander' THEN events.id
                                                                          ELSE NULL END),0)              AS commanders_changed
      , MAX(ic.timeline_tab_clicks) + COALESCE(COUNT(CASE 
                  WHEN event = 'frontend' AND action = 'timeline_tab_clicked' THEN events.id 
                                                                              ELSE NULL END), 0)         AS timeline_tab_clicks
      , MAX(ic.ended_incident) + COALESCE(COUNT(CASE 
                  WHEN event = 'incident' AND action = 'end' THEN events.id ELSE NULL END),0)            AS ended_incident
      , MAX(ic.restarted_incident) + COALESCE(COUNT(
                CASE WHEN event = 'incident' AND action = 'restart' THEN events.id ELSE NULL END),0)     AS restarted_incident
      , MAX(ic.task_assignees_set) + COALESCE(COUNT(CASE
                  WHEN event = 'tasks' AND action = 'set_assignee_for_task' THEN events.id
                                                                            ELSE NULL END),0)            AS task_assignees_set
      , MAX(ic.task_states_modified) + COALESCE(COUNT(CASE
                  WHEN event = 'tasks' AND action = 'modify_task_state' THEN events.id
                                                                        ELSE NULL END),0)                AS task_states_modified
      , MAX(ic.tasks_added) + COALESCE(COUNT(CASE 
                  WHEN event = 'tasks' AND action = 'add_task' THEN events.id ELSE NULL END),0)                AS tasks_added
      , MAX(ic.tasks_removed) + COALESCE(COUNT(CASE 
                  WHEN event = 'tasks' AND action = 'remove_task' THEN events.id ELSE NULL END),0)             AS tasks_removed
      , MAX(ic.tasks_renamed) + COALESCE(COUNT(CASE 
                  WHEN event = 'tasks' AND action = 'rename_task' THEN events.id ELSE NULL END),0)             AS tasks_renamed
      , MAX(ic.task_slash_commands_run) + COALESCE(COUNT(CASE
                  WHEN event = 'tasks' AND action = 'run_task_slash_command' THEN events.id
                                                                             ELSE NULL END),0)           AS task_slash_commands_run
      , MAX(ic.tasks_moved) + COALESCE(COUNT(CASE 
                  WHEN event = 'tasks' AND action = 'move_task' THEN events.id ELSE NULL END),0)               AS tasks_moved
    FROM {{this}} ic
    JOIN {{ ref('incident_response_events') }} events
        ON {{ dbt_utils.surrogate_key(['COALESCE(events.incident_id, events.incidentid)',
                 'COALESCE(events.user_id, events.anonymous_id)'])}} = ic.id
        AND events.timestamp > ic.last_activity_date
    LEFT JOIN min_user_date mud
        ON ic.incident_id = mud.incident_id
        AND ic.server_id = mud.server_id
        AND mud.first_activity_date > ic.last_activity_date 
    WHERE events.event in ('task', 'incident', 'frontend')
    AND events.timestamp::DATE <= CURRENT_TIMESTAMP
    AND COALESCE(events.incident_id, events.incidentid) IS NOT NULL
    GROUP BY 1, 2, 3

    UNION all

    SELECT
        {{ dbt_utils.surrogate_key(['COALESCE(events.incident_id, events.incidentid)',
                 'COALESCE(events.user_id, events.anonymous_id)'])}}                                                 AS id
      , COALESCE(events.incident_id, events.incidentid)                                                             AS incident_id
      , COALESCE(events.user_id, events.anonymous_id)                                                               AS server_id
      , MAX(COALESCE(events.playbook_id, events.playbookid))                                                        AS playbook_id
      , MIN(events.timestamp)                                                                         AS created_date
      , MAX(events.timestamp)                                                                         AS last_activity_date
      , MIN(CASE WHEN currentstatus = 'Resolved' THEN events.timestamp ELSE NULL END)                 AS resolved_date
      , MIN(CASE WHEN currentstatus = 'Archived' THEN events.timestamp ELSE NULL END)                 AS archived_date
      , MAX(CASE WHEN currentstatus = 'Reported' THEN TRUE ELSE FALSE END)                            AS reported
      , MAX(CASE WHEN currentstatus = 'Active' THEN TRUE ELSE FALSE END)                              AS acknowledged
      , MAX(CASE WHEN currentstatus = 'Resolved' THEN TRUE ELSE FALSE END)                            AS resolved
      , MAX(CASE WHEN currentstatus = 'Archived' THEN TRUE ELSE FALSE END)                            AS archived
      , COUNT(DISTINCT COALESCE(events.useractualid, events.user_actual_id))                          AS incident_users
      , COUNT(CASE
                  WHEN event = 'incident' AND action = 'update_status' THEN events.id
                                                                       ELSE NULL END)                 AS status_updates
      , COUNT(CASE
                  WHEN event = 'incident' AND action = 'change_stage' THEN events.id
                                                                      ELSE NULL END)                  AS stages_changed
      , COUNT(CASE
                  WHEN event = 'incident' AND action = 'add_timeline_event_from_post' THEN events.id
                                                                                      ELSE NULL END)  AS timeline_events_added
      , COUNT(CASE
                  WHEN event = 'incident' AND action = 'change_commander' THEN events.id
                                                                          ELSE NULL END)              AS commanders_changed
      , COUNT(CASE WHEN event = 'frontend' AND action = 'timeline_tab_clicked' THEN events.id ELSE NULL END) AS ended_incident
      , COUNT(CASE WHEN event = 'incident' AND action = 'end' THEN events.id ELSE NULL END)                  AS timeline_tab_clicks
      , COUNT(
                CASE WHEN event = 'incident' AND action = 'restart' THEN events.id ELSE NULL END)            AS restarted_incident
      , COUNT(CASE
                  WHEN event = 'tasks' AND action = 'set_assignee_for_task' THEN events.id
                                                                            ELSE NULL END)            AS task_assignees_set
      , COUNT(CASE
                  WHEN event = 'tasks' AND action = 'modify_task_state' THEN events.id
                                                                        ELSE NULL END)                AS task_states_modified
      , COUNT(CASE WHEN event = 'tasks' AND action = 'add_task' THEN events.id ELSE NULL END)                AS tasks_added
      , COUNT(CASE WHEN event = 'tasks' AND action = 'remove_task' THEN events.id ELSE NULL END)             AS tasks_removed
      , COUNT(CASE WHEN event = 'tasks' AND action = 'rename_task' THEN events.id ELSE NULL END)             AS tasks_renamed
      , COUNT(CASE
                  WHEN event = 'tasks' AND action = 'run_task_slash_command' THEN events.id
                                                                             ELSE NULL END)           AS task_slash_commands_run
      , COUNT(CASE WHEN event = 'tasks' AND action = 'move_task' THEN events.id ELSE NULL END)               AS tasks_moved
    FROM {{ ref('incident_response_events') }} events
    WHERE COALESCE(events.incident_id, events.incidentid) not in (SELECT incident_id FROM {{this}})
    AND events.event in ('task', 'incident', 'frontend')
    AND events.timestamp::DATE <= CURRENT_TIMESTAMP
    AND COALESCE(events.incident_id, events.incidentid) IS NOT NULL
    GROUP BY 1, 2, 3
),

incident_fact AS (
SELECT incident_details.*
    , MAX(COALESCE(events.serverversion, events.server_version)) AS server_version
    , MAX(COALESCE(events.pluginversion, events.plugin_version)) AS plugin_version
FROM incident_details
JOIN {{ ref('incident_response_events') }} events
    ON incident_details.incident_id = events.incident_id
    AND incident_details.last_activity_date = events.timestamp
{{ dbt_utils.group_by(n=27)}}
)

SELECT *
FROM incident_fact

{% else %}

incident_details AS (
    SELECT
        {{ dbt_utils.surrogate_key(['COALESCE(events.incident_id, events.incidentid)',
                 'COALESCE(events.user_id, events.anonymous_id)'])}}                                                 AS id
      , COALESCE(events.incident_id, events.incidentid)                                                             AS incident_id
      , COALESCE(events.user_id, events.anonymous_id)                                                               AS server_id
      , MAX(COALESCE(events.playbook_id, events.playbookid))                                                        AS playbook_id
      , MIN(events.timestamp)                                                                         AS created_date
      , MAX(events.timestamp)                                                                         AS last_activity_date
      , MIN(CASE WHEN currentstatus = 'Resolved' THEN events.timestamp ELSE NULL END)                 AS resolved_date
      , MIN(CASE WHEN currentstatus = 'Archived' THEN events.timestamp ELSE NULL END)                 AS archived_date
      , MAX(CASE WHEN currentstatus = 'Reported' THEN TRUE ELSE FALSE END)                            AS reported
      , MAX(CASE WHEN currentstatus = 'Active' THEN TRUE ELSE FALSE END)                              AS acknowledged
      , MAX(CASE WHEN currentstatus = 'Resolved' THEN TRUE ELSE FALSE END)                            AS resolved
      , MAX(CASE WHEN currentstatus = 'Archived' THEN TRUE ELSE FALSE END)                            AS archived
      , COUNT(DISTINCT COALESCE(events.useractualid, events.user_actual_id))                          AS incident_users
      , COUNT(CASE
                  WHEN event = 'incident' AND action = 'update_status' THEN events.id
                                                                       ELSE NULL END)                 AS status_updates
      , COUNT(CASE
                  WHEN event = 'incident' AND action = 'change_stage' THEN events.id
                                                                      ELSE NULL END)                  AS stages_changed
      , COUNT(CASE
                  WHEN event = 'incident' AND action = 'add_timeline_event_from_post' THEN events.id
                                                                                      ELSE NULL END)  AS timeline_events_added
      , COUNT(CASE
                  WHEN event = 'incident' AND action = 'change_commander' THEN events.id
                                                                          ELSE NULL END)              AS commanders_changed
      , COUNT(CASE WHEN event = 'frontend' AND action = 'timeline_tab_clicked' THEN events.id ELSE NULL END) AS timeline_tab_clicks
      , COUNT(CASE WHEN event = 'incident' AND action = 'end' THEN events.id ELSE NULL END)                  AS ended_incident
      , COUNT(
                CASE WHEN event = 'incident' AND action = 'restart' THEN events.id ELSE NULL END)            AS restarted_incident
      , COUNT(CASE
                  WHEN event = 'tasks' AND action = 'set_assignee_for_task' THEN events.id
                                                                            ELSE NULL END)            AS task_assignees_set
      , COUNT(CASE
                  WHEN event = 'tasks' AND action = 'modify_task_state' THEN events.id
                                                                        ELSE NULL END)                AS task_states_modified
      , COUNT(CASE WHEN event = 'tasks' AND action = 'add_task' THEN events.id ELSE NULL END)                AS tasks_added
      , COUNT(CASE WHEN event = 'tasks' AND action = 'remove_task' THEN events.id ELSE NULL END)             AS tasks_removed
      , COUNT(CASE WHEN event = 'tasks' AND action = 'rename_task' THEN events.id ELSE NULL END)             AS tasks_renamed
      , COUNT(CASE
                  WHEN event = 'tasks' AND action = 'run_task_slash_command' THEN events.id
                                                                             ELSE NULL END)           AS task_slash_commands_run
      , COUNT(CASE WHEN event = 'tasks' AND action = 'move_task' THEN events.id ELSE NULL END)               AS tasks_moved
    FROM {{ ref('incident_response_events') }} events
    WHERE events.event in ('task', 'incident', 'frontend')
    AND events.timestamp::DATE <= CURRENT_TIMESTAMP
    AND COALESCE(events.incident_id, events.incidentid) IS NOT NULL
    GROUP BY 1, 2, 3
                         ),

incident_fact AS (
SELECT incident_details.*
    , MAX(COALESCE(events.serverversion, events.server_version)) AS server_version
    , MAX(COALESCE(events.pluginversion, events.plugin_version)) AS plugin_version
FROM incident_details
JOIN {{ ref('incident_response_events') }} events
    ON incident_details.incident_id = events.incident_id
    AND incident_details.last_activity_date = events.timestamp
{{ dbt_utils.group_by(n=27)}}
)

SELECT *
FROM incident_fact

{% endif %}