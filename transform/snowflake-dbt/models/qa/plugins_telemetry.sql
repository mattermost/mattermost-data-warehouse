{{config({
    "materialized": "incremental",
    "schema": "qa",
    "tags":"hourly"
  })
}}

{% set rudder_relations = get_rudder_relations(schema=["mm_plugin_dev", "mm_plugin_rc", "mm_plugin_qa"], database='RAW') %}
{{ union_relations(relations = rudder_relations) }}