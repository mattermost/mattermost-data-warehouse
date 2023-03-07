{% macro join_tracks_event_tables(schema, columns=[]) -%}
    {%-
        set rudderstack_tables = ['IDENTIFIES', 'USERS', 'TRACKS', 'PAGES', 'SCREENS', 'GROUPS', 'ALIASES', 'RUDDER_DISCARDS']
    -%}
    {%- set all_relations = dbt_utils.get_relations_by_pattern(schema, '%', database='RAW') | list -%}
    {%- set relations = all_relations | rejectattr('identifier', 'in', rudderstack_tables) | list -%}
    {%- set include = get_base_event_columns() -%}

    {%- if columns -%}
        {# If user defined columns to keep, filter them #}
        {{
            dbt_utils.union_relations(
                relations=relations,
                include=columns,
                source_column_name=None
            )
        }}
    {%- else -%}
        {# Keep all columns #}
        {{
            dbt_utils.union_relations(
                relations=relations,
                source_column_name=None
            )
        }}
    {%- endif -%}

{%- endmacro%}