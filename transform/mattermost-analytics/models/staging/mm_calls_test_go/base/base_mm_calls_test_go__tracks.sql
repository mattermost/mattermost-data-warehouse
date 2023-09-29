{%- set include_columns = ["call_id", "actual_user_id", "participants", "server_version", "duration", 
"screen_duration", "plugin_build", "plugin_version"] -%}

{%- set relations = get_event_relations('mm_calls_test_go', database='RAW') -%}

{{
    dbt_utils.union_relations(
        relations=relations,
        include=get_base_event_columns() + include_columns,
        source_column_name=None
    )
}}