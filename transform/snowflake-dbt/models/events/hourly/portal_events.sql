{{config({
    "materialized": "incremental",
    "schema": "events",
    "tags":"hourly",
    "unique_key":"id"
  })
}}

{% set rudder_relations = get_rudder_relations(schema=["portal_prod"], database='RAW') %}
{{ union_relations(relations = rudder_relations) }}