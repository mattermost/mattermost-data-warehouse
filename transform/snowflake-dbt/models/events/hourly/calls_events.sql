{{config({
    "materialized": "incremental",
    "schema": "events",
    "tags":"preunion"
  })
}}

{% set rudder_relations = get_rudder_relations(schema=["mm_calls_test_go"], database='RAW') %}
{{ union_relations(relations = rudder_relations[0], tgt_relation = rudder_relations[1]) }}