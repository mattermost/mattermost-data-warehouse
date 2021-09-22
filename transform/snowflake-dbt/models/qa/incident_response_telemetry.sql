{{config({
    "materialized": "incremental",
    "schema": "qa",
    "tags":"hourly"
  })
}}

{% set rudder_relations = get_rudder_relations(schema=["incident_response_dev"], database='RAW') %}
{{ union_relations(relations = rudder_relations[0], tgt_relation = rudder_relations[1]) }}