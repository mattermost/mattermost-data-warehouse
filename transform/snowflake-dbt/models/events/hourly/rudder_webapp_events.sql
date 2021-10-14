{{config({
    "materialized": "incremental",
    "schema": "events",
    "tags":"preunion",
    "snowflake_warehouse": "transform_m"
  })
}}

{% set rudder_relations = get_rudder_relations(schema=["mm_telemetry_prod"], database='RAW', 
                          table_inclusions="'event'") %}
{{ union_relations(relations = rudder_relations[0], tgt_relation = rudder_relations[1]) }}


