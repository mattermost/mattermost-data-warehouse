{{config({
    "materialized": "incremental",
    "schema": "events",
    "tags":"hourly"
  })
}}

{% set rudder_relations = get_rudder_relations(schema=["incident_response_prod"], database='RAW', table_exclusions="'add_checklist_item', 'change_commander', 'change_stage', 'check_checklist_item', 'create_incident', 'create_playbook', 'delete_playbook', 'end_incident', 'modify_state_checklist_item', 'move_checklist_item', 'remove_checklist_item', 'rename_checklist_item', 'restart_incident', 'run_checklist_item_slash_command', 'set_assignee', 'uncheck_checklist_item',  'update_playbook'") %}

{{ union_relations(relations = rudder_relations[0], tgt_relation = rudder_relations[1]) }}