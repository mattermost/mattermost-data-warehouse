{%- set extra_columns = [ "context_useragent", "context_user_agent"] -%}

{%- set relations = get_event_relations('mm_telemetry_prod', database='RAW', exclude=['event']) -%}

{{
    dbt_utils.union_relations(
        relations=relations + [ref('base_events_merged')],
        include=get_base_event_columns() + extra_columns,
        source_column_name=None
    )
}}