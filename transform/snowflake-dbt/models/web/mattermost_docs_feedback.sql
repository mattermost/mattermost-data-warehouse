{{config({
    "materialized": 'incremental',
    "schema": "web",
    "tags":"hourly"
  })
}}

{% set rudder_relations = get_rudder_relations(schema=["mattermost_docs"], database='RAW', table_inclusions="'feedback_submitted'") %}
{{ union_relations(relations = rudder_relations[0], tgt_relation = rudder_relations[1]) }}