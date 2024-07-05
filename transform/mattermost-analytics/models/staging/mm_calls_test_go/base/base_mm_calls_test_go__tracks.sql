{%- set include_columns = [
    "actual_user_id", "server_version", "plugin_build", "plugin_version", "participant_id",
    "context_feature_name", "context_feature_skus"
] -%}

{%- set relations = get_event_relations('mm_calls_test_go', database='RAW') -%}

{{
    dbt_utils.union_relations(
        relations=relations,
        include=get_base_event_columns() + include_columns,
        source_column_name=None
    )
}}