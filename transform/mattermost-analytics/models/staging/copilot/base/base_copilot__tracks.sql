-- Columns used for calculating feature usage or active users.
{%- set include_columns = [
    "actual_user_id", "context_feature_name", "context_feature_skus",
] -%}

-- Extra columns appearing in most tables.
{%- set extra_columns = [
     "plugin_version", "plugin_build", "server_version", "bot_id", "bot_service_type"
] -%}

{%- set relations = get_event_relations('copilot_plugin_prod', database='RAW', min_rows=1) -%}

{{
    dbt_utils.union_relations(
        relations=relations,
        include=get_base_event_columns() + include_columns + extra_columns,
        source_column_name=None
    )
}}