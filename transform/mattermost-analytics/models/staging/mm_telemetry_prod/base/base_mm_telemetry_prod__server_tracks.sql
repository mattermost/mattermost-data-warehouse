{%- set extra_columns = [ "context_useragent", "context_user_agent", "context_feature_name", "context_feature_skus"] -%}

{%- set relations = get_event_relations('mm_telemetry_prod', database='RAW', exclude=['event']) -%}

{{
    dbt_utils.union_relations(
        relations=relations,
        include=get_base_event_columns() + extra_columns,
        source_column_name=None
    )
}}