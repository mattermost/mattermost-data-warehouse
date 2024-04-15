{%- set extra_columns = ["context_user_agent"] -%}

{%- set relations = get_event_relations('mattermost2', database='RAW') -%}

{{
    dbt_utils.union_relations(
        relations=relations,
        include=get_base_event_columns() + extra_columns,
        source_column_name=None
    )
}}