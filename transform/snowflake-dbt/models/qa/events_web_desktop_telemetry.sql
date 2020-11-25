{{config({
    "materialized": "incremental",
    "schema": "qa",
    "tags":"hourly",
    "unique_key":"id"
  })
}}

{% set rudder_relations = get_rudder_relations(schema=["mm_telemetry_qa", "mm_telemetry_rc", "portal_test"], database='RAW', table_inclusions="'event'") %}
{{ union_relations(relations = rudder_relations) }}