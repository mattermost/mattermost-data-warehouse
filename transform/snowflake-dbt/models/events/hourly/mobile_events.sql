{{config({
    "materialized": "incremental",
    "schema": "events",
    "tags":"hourly"
  })
}}

{% set rudder_relations = get_rudder_relations(schema=["mm_mobile_prod"], database='RAW') %}
{{ union_relations(relations = rudder_relations) }}