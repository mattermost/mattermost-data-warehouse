{{config({
    "materialized": "incremental",
    "schema": "bizops"
  })
}}

{% set rudder_relations = get_rudder_relations('mm_mobile_prod', 'RAW') %}
{{ union_relations(relations = rudder_relations) }}