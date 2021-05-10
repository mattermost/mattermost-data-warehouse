{{config({
    "materialized": "incremental",
    "schema": "events",
    "tags":"preunion",
    "snowflake_warehouse": "transform_l",
  })
}}

{% set rudder_relations = get_rudder_relations(schema=["mm_telemetry_prod","mm_telemetry_rc"], database='RAW',table_inclusions="'config_feature_flags'") %}
{{ union_relations(relations = rudder_relations) }}