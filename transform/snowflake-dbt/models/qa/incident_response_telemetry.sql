{{config({
    "materialized": "table",
    "schema": "qa",
    "tags":"hourly",
    "unique_key":"id"
  })
}}

{% set rudder_relations = get_rudder_relations(schema=["incident_response_dev"], database='RAW') %}

{{ union_relations(relations = rudder_relations) }}