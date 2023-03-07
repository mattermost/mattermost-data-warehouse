
{%- set relations = get_event_relations('mm_telemetry_prod', database='RAW') -%}

{{
    dbt_utils.union_relations(
        relations=relations,
        include=get_base_event_columns(),
        source_column_name=None
    )
}}