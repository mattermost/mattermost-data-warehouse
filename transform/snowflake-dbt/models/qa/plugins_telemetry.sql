{{config({
    "materialized": "incremental",
    "schema": "qa",
    "tags":"hourly"
  })
}}

{% set rudder_relations = get_rudder_relations(schema=["mm_plugin_dev", "mm_plugin_rc", "mm_plugin_qa"], database='RAW', table_exclusions="'event'") %}
{{ union_relations(relations = rudder_relations[0], tgt_relation = rudder_relations[1]) }}