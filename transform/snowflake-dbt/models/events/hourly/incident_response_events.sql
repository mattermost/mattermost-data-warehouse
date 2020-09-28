{{config({
    "materialized": "incremental",
    "schema": "events",
    "tags":"preunion",
    "unique_key":"id"
  })
}}

{% set rudder_relations = get_rudder_relations(schema=["incident_response_prod"], database='RAW') %}
{{ union_relations(relations = rudder_relations) }}