{{config({
    "materialized": "incremental",
    "schema": "qa",
    "tags":"hourly"
  })
}}

{% set rudder_relations = get_rudder_relations(schema=["mm_telemetry_qa", "mm_telemetry_rc"], database='RAW', table_exclusions="'event'") %}

{{ union_relations(relations = rudder_relations) }}