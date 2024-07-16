{%- set include_columns = [
    "actual_user_id", "context_feature_name", "context_feature_skus"
] -%}

{%- set relations = get_event_relations('copilot_plugin_prod', database='RAW', min_rows=1) -%}

{{
    dbt_utils.union_relations(
        relations=relations,
        include=get_base_event_columns() + include_columns,
        source_column_name=None
    )
}}