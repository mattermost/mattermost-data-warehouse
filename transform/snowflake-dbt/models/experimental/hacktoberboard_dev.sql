{{config({
    "materialized": "incremental",
    "schema": "experimental",
    "tags":"nightly"
  })
}}

{% set rudder_relations = get_rudder_relations(schema=["hacktoberboard_dev"], database='RAW') %}
{{ union_relations(relations = rudder_relations) }}