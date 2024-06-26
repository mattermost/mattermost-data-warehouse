{{config({
    "materialized": "incremental",
    "schema": "events",
    "tags": ["nightly"],
    "snowflake_warehouse": "transform_l"
  })
}}

{% set rudder_relations = get_rudder_relations(schema=["mm_mobile_prod"], database='RAW',table_exclusions="'api_profiles_get_in_channel','api_profiles_get_by_usernames','api_profiles_get_by_ids'") %}
{{ union_relations(relations = rudder_relations[0], tgt_relation = rudder_relations[1]) }}