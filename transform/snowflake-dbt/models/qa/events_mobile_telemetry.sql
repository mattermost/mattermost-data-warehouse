{{config({
    "materialized": "incremental",
    "schema": "qa",
    "tags": ["hourly", "deprecated"]
  })
}}

{% set rudder_relations = get_rudder_relations(schema=["mm_mobile_test", "mm_mobile_beta"], database='RAW') %}
{{ union_relations(relations = rudder_relations[0], tgt_relation = rudder_relations[1]) }}