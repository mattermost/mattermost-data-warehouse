{{config({
    "materialized": "incremental",
    "schema": "events",
    "tags":"hourly"
  })
}}

{% set rudder_relations = get_rudder_relations('mm_mobile_prod', 'RAW') %}
{{ union_relations(relations = rudder_relations) }}