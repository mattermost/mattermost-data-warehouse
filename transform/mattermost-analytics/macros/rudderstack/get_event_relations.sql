{% macro get_potential_event_tables(schema_pattern, min_rows=100, database=target.database) %}
    -- Load potential event table by applying heuristics (schema name and minimum number or rows) .
    -- Based on https://github.com/dbt-labs/dbt-utils/blob/main/macros/sql/get_tables_by_pattern_sql.sql
    SELECT DISTINCT
        table_schema AS {{ adapter.quote('table_schema') }},
        table_name AS {{ adapter.quote('table_name') }},
        {{ dbt_utils.get_table_types_sql() }}
    FROM
        {{ database }}.information_schema.tables
    WHERE
        table_schema ILIKE '{{ schema_pattern }}'
        AND row_count >= {{min_rows}}
{% endmacro %}


{% macro get_event_relations(schema_pattern, min_rows=100, database=target.database, exclude=[]) -%}
    -- Based on https://github.com/dbt-labs/dbt-utils/blob/main/macros/sql/get_relations_by_pattern.sql.
    {%- call statement('get_tables', fetch_result=True) %}

        {{ get_potential_event_tables(schema_pattern, min_rows=min_rows, database=database) }}

    {%- endcall -%}


    {%- set table_list = load_result('get_tables') -%}
    {%-
        set rudderstack_tables = ['IDENTIFIES', 'USERS', 'TRACKS', 'PAGES', 'SCREENS', 'GROUPS', 'ALIASES', 'RUDDER_DISCARDS']
    -%}
    {%- set all_excludes = rudderstack_tables -%}

    {%- if exclude -%}
        {%- for exc in exclude -%}
            {%- do all_excludes.append(exc | upper) -%}
        {%- endfor -%}
    {%- endif -%}

    {%- if table_list and table_list['table'] -%}
        {%- set tbl_relations = [] -%}
        {%- for row in table_list['table'] -%}
            {%-if row.table_name.upper() not in all_excludes -%}
                {%- set tbl_relation = api.Relation.create(
                    database=database,
                    schema=row.table_schema,
                    identifier=row.table_name,
                    type=row.table_type
                ) -%}
                {%- do tbl_relations.append(tbl_relation) -%}
            {%- endif -%}
        {%- endfor -%}

        {{ return(tbl_relations) }}
    {%- else -%}
        {{ return([]) }}
    {%- endif -%}
{%- endmacro%}