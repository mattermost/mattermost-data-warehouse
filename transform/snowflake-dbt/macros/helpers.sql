{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    {%- set prefix = target.name + '_' if target.name != 'prod' else '' -%}

    {%- if custom_schema_name is none -%}

        {{prefix}}{{ default_schema }}

    {%- else -%}

        {{prefix}}{{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}

{% macro get_sys_var(var_name) -%}

select get_sys_var({{ var_name }})

{%- endmacro %}

{% macro post_audit_delete_hook(audit_table) %}
    {% set orgm_tables = ['account', 'campaign', 'campaignmember', 'contact', 'lead', 'opportunity', 'opportunitylineitem', 'territory__c', 'territory_mapping__c'] %}

    {% for t in orgm_tables %}
        {{ delete_orgm_rows(audit_table, t) }}
    {% endfor %}
{% endmacro %}

{% macro delete_orgm_rows(audit_table, orgm_table) %}
    {% set query %}
        delete from {{ source('orgm', orgm_table) }}
        where sfid
            in (select deleted_sfid from {{ audit_table }})
    {% endset %}

    {% do run_query(query) %}
{% endmacro %}

{% macro pg_import(destination_table, script) %}
    {% set query %}
        insert into analytics.util.pg_imports (source_table, destination_table, created_at, post_process_file)
        values ('{{ this.schema }}.{{ this.table }}', '{{ destination_table }}', current_timestamp, '{{ script }}')
    {% endset %}

    {% do run_query(query) %}
{% endmacro %}

{% macro resume_warehouse(warehouse) %}
    alter warehouse {{warehouse}} resume if suspended
{% endmacro %}

{% macro suspend_warehouse(run, warehouse) %}
    alter warehouse {{warehouse}} suspend
{% endmacro %}

{% macro get_rudder_track_tables(schema, database=target.database, table_exclusions=table_exclusions, table_inclusions=table_inclusions) %}
    {% for scheme in schema %}
    select distinct
        table_schema as "table_schema", table_name as "table_name"
    from {{database}}.information_schema.tables
    WHERE table_schema ilike '{{ scheme }}'
        {% if table_inclusions != "'pages'" %}
        AND table_name not in ('TRACKS', 'USERS', 'SCREENS', 'IDENTIFIES', 'PAGES', 'RUDDER_DISCARDS')
        {% endif %}
    {%- if table_exclusions -%}

     and lower(table_name) not in ({{ table_exclusions}})
     
    {%- endif -%}
    {%- if table_inclusions and scheme != 'portal_test' -%}

     and lower(table_name) in ({{ table_inclusions}})
     
    {%- endif -%}
    {% if not loop.last %} UNION ALL {% endif %}
    {% endfor %}

{% endmacro %}

{% macro get_rudder_relations(schema, database=target.database, table_exclusions="", table_inclusions="") %}

    {%- call statement('get_tables', fetch_result=True) %}

      {{ get_rudder_track_tables(schema, database, table_exclusions=table_exclusions, table_inclusions=table_inclusions) }}

    {%- endcall -%}

    {%- set table_list = load_result('get_tables') -%}

    {%- if table_list and table_list['table'] -%}
        {%- set tbl_relations = [] -%}
        {%- for row in table_list['table'] -%}
            {%- set tbl_relation = api.Relation.create(database, row.table_schema, row.table_name) -%}
            {%- do tbl_relations.append(tbl_relation) -%}
        {%- endfor -%}

        {{ return(tbl_relations) }}
    {%- else -%}
        {{ return([]) }}
    {%- endif -%}

{% endmacro %}

{%- macro union_relations(relations, column_override=none, include=[], exclude=[], source_column_name=none) -%}

    {%- if exclude and include -%}
        {{ exceptions.raise_compiler_error("Both an exclude and include list were provided to the `union` macro. Only one is allowed") }}
    {%- endif -%}

    {#-- Prevent querying of db in parsing mode. This works because this macro does not create any new refs. -#}
    {%- if not execute %}
        {{ return('') }}
    {% endif -%}

    {%- set column_override = column_override if column_override is not none else {} -%}
    {%- set source_column_name = source_column_name if source_column_name is not none else '_dbt_source_relation' -%}

    {%- set relation_columns = {} -%}
    {%- set column_superset = {} -%}

    {%- for relation in relations -%}

        {%- do relation_columns.update({relation: []}) -%}

        {%- do dbt_utils._is_relation(relation, 'union_relations') -%}
        {%- set cols = adapter.get_columns_in_relation(relation) -%}
        {%- for col in cols -%}

        {#- If an exclude list was provided and the column is in the list, do nothing -#}
        {%- if exclude and col.column in exclude -%}

        {#- If an include list was provided and the column is not in the list, do nothing -#}
        {%- elif include and col.column not in include -%}

        {#- Otherwise add the column to the column superset -#}
        {%- else -%}

            {#- update the list of columns in this relation -#}
            {%- do relation_columns[relation].append(col.column) -%}

            {%- if col.column in column_superset -%}

                {%- set stored = column_superset[col.column] -%}
                {%- if col.is_string() and stored.is_string() and col.string_size() > stored.string_size() -%}

                    {%- do column_superset.update({col.column: col}) -%}

                {%- endif %}

            {%- else -%}

                {%- do column_superset.update({col.column: col}) -%}

            {%- endif -%}

        {%- endif -%}

        {%- endfor -%}
    {%- endfor -%}

    {%- set ordered_column_names = column_superset.keys() -%}

    {%- if is_incremental() -%}
        with
        --

        {%- if this.table == 'user_events_telemetry' -%}
        --
        max_time AS (
                 SELECT
                    MAX(timestamp) - INTERVAL '12 HOURS' as max_time
                 FROM {{ this }} 
                 WHERE {{this}}.timestamp <= CURRENT_TIMESTAMP
             ), 

        join_key AS (
                SELECT 
                    id as join_id
                  , _dbt_source_relation2
                FROM {{ this }}
                JOIN max_time mt
                    ON {{ this }}.timestamp >= mt.max_time
                    AND {{this}}.timestamp <= CURRENT_TIMESTAMP
             ),
        {%- elif this.table == 'mobile_events' -%}
             --
             max_time AS (
                 SELECT 
                    MAX(timestamp) - INTERVAL '12 HOURS' AS max_time
                 FROM {{ this }} 
                 WHERE timestamp <= CURRENT_TIMESTAMP
             ),

             join_key AS (
                    SELECT 
                        id as join_id
                      , _dbt_source_relation
                    FROM {{ this }}
                    JOIN max_time mt
                        ON {{ this }}.timestamp >= mt.max_time 
                        AND {{ this }}.timestamp <= CURRENT_TIMESTAMP
             ),

            {%- elif adapter.quote(relation)[7:28] == 'MM_PLUGIN_DEV.NPS_NPS' %}
             --
             max_time AS (
                 SELECT 
                    MAX(original_timestamp) - INTERVAL '3 HOURS' AS max_time
                 FROM {{ this }} 
                 WHERE original_timestamp <= CURRENT_TIMESTAMP
             ),

             join_key AS (
                    SELECT 
                        id as join_id
                      , _dbt_source_relation
                    FROM {{ this }}
                    JOIN max_time mt
                        ON {{ this }}.original_timestamp >= mt.max_time 
                        AND {{this}}.original_timestamp <= CURRENT_TIMESTAMP
             ),

            {%- elif this.schema == 'qa'  %}
             --
             max_time AS (
                 SELECT 
                    MAX(original_timestamp) -  INTERVAL '3 HOURS' AS max_time
                 FROM {{ this }} 
                 WHERE original_timestamp <= CURRENT_TIMESTAMP
             ),

             join_key AS (
                    SELECT 
                        id as join_id
                      , _dbt_source_relation
                    FROM {{ this }}
                    JOIN max_time mt
                        ON {{ this }}.original_timestamp >= mt.max_time 
                        AND {{this}}.original_timestamp <= CURRENT_TIMESTAMP
             ),

            {%- elif this.schema == 'web'  %}
             --
             max_time AS (
                 SELECT 
                    MAX(timestamp) - INTERVAL '3 HOURS' AS max_time
                 FROM {{ this }} 
                 WHERE timestamp <= CURRENT_TIMESTAMP
             ),

             join_key AS (
                    SELECT 
                        id as join_id
                      , _dbt_source_relation
                    FROM {{ this }}
                    JOIN max_time mt
                        ON {{ this }}.timestamp >= mt.max_time 
                        AND {{this}}.timestamp <= CURRENT_TIMESTAMP
             ),

             {%- else -%}
             --
             max_time AS (
                 SELECT 
                    MAX(timestamp) - INTERVAL '12 HOURS' AS max_time
                 FROM {{ this }} 
                 WHERE timestamp <= CURRENT_TIMESTAMP
             ),

             join_key AS (
                    SELECT 
                        id as join_id
                      , _dbt_source_relation
                    FROM {{ this }}
                    JOIN max_time mt
                        ON {{ this }}.timestamp >= mt.max_time 
                        AND {{this}}.timestamp <= CURRENT_TIMESTAMP
             ), 
        
            {%- endif -%}

    {%- endif -%}

    {%- if not is_incremental() -%}
    with 
    --
    {%- elif is_incremental() -%}
    --
    {%- endif -%}

    {%- for relation in relations %}
               {%- if this.table == 'daily_website_traffic' -%}
            --
               {{ ((((["'", relation, "'"]|join).split('.')[1]))|replace("'", ""))|lower }}_{{ ((((["'", relation, "'"]|join).split('.')[2]))|replace("'", ""))|lower }} AS (
        
               {%- else -%}
            --
               {{ ((((["'", relation, "'"]|join).split('.')[2]))|replace("'", ""))|lower }} AS (

               {%- endif -%}
               --    
               select

                cast({{ dbt_utils.string_literal(relation) }} as {{ dbt_utils.type_string() }}) as {{ source_column_name if ('_DBT_SOURCE_RELATION' not in column_superset) else '_DBT_SOURCE_RELATION2'}},
                {% for col_name in ordered_column_names -%}

                    {%- set col = column_superset[col_name] -%}
                    {%- set col_type = column_override.get(col.column, col.data_type) -%}
                    {%- set col_name = adapter.quote(col_name) if col_name in relation_columns[relation] else 'null' -%}
                    
                        {%- if col.quoted[-10:-1] == 'TIMESTAMP' and col.quoted[1:3] == 'NPS' -%}

                        cast({{ relation }}.ORIGINAL_TIMESTAMP as DATE) as {{ col.quoted }}

                        {%- else -%}
                        cast({{ col_name }} as {{ col_type }}) as {{ col.quoted }}

                        {%- endif -%}
                        {%- if not loop.last -%},{%- elif loop.last -%}
                        {%- endif -%}

                {%- endfor -%} 
              from {{ relation }}
              {% if is_incremental() and this.table == 'user_events_telemetry' %}
                JOIN max_time mt
                    ON {{ relation }}.timestamp >= mt.max_time
                LEFT JOIN join_key a
                    ON {{ relation }}.id = a.join_id
                    AND a._dbt_source_relation2 = {{ ["'", relation, "'"]|join }}
                WHERE timestamp <= CURRENT_TIMESTAMP
                AND a.join_id is null
            {% elif is_incremental() and this.table == 'mobile_events' %}
                JOIN max_time mt
                    ON {{ relation }}.timestamp >= mt.max_time
                LEFT JOIN join_key a
                    ON {{ relation }}.id = a.join_id
                    AND a._dbt_source_relation = {{ ["'", relation, "'"]|join }}
                WHERE timestamp <= CURRENT_TIMESTAMP
                AND a.join_id is null 
            {% elif is_incremental() and adapter.quote(relation)[7:28] == 'MM_PLUGIN_DEV.NPS_NPS' %}
                JOIN max_time mt
                    ON {{ relation }}.original_timestamp >= mt.max_time
                LEFT JOIN join_key a
                    ON {{ relation }}.id = a.join_id
                    AND a._dbt_source_relation = {{ ["'", relation, "'"]|join }}
                WHERE original_timestamp <= CURRENT_TIMESTAMP
                AND a.join_id is null
            {% elif is_incremental() and this.schema == 'qa' %}
                JOIN max_time mt
                    ON {{ relation }}.original_timestamp >= mt.max_time
                LEFT JOIN join_key a
                    ON {{ relation }}.id = a.join_id
                    AND a._dbt_source_relation = {{ ["'", relation, "'"]|join }}
                WHERE original_timestamp <= CURRENT_TIMESTAMP
                AND a.join_id is null
            {% elif is_incremental() and this.schema == 'web' %}
                JOIN max_time mt
                    ON {{ relation }}.timestamp >= mt.max_time
                LEFT JOIN join_key a
                    ON {{ relation }}.id = a.join_id
                    AND a._dbt_source_relation = {{ ["'", relation, "'"]|join }}
                WHERE timestamp <= CURRENT_TIMESTAMP
                AND a.join_id is null
            {% elif is_incremental() %}
                JOIN max_time mt
                    ON {{ relation }}.timestamp >= mt.max_time
                LEFT JOIN join_key a
                    ON {{ relation }}.id = a.join_id
                    AND a._dbt_source_relation = {{ ["'", relation, "'"]|join }}
                WHERE timestamp <= CURRENT_TIMESTAMP
                AND a.join_id is null 
            {% endif %}

            ){% if not loop.last -%},{% endif %}

    {%- endfor -%}

        {%- for relation in relations %}
        {%- if this.table == 'daily_website_traffic' -%}
        (
            --
            select
               {{ ((((["'", relation, "'"]|join).split('.')[1]))|replace("'", ""))|lower }}_{{ ((((["'", relation, "'"]|join).split('.')[2]))|replace("'", ""))|lower }}.*
            --
            from {{ ((((["'", relation, "'"]|join).split('.')[1]))|replace("'", ""))|lower }}_{{ ((((["'", relation, "'"]|join).split('.')[2]))|replace("'", ""))|lower }} 
            --
        )
               {%- else -%}
            --
        (
            --
            select
                {{ ((((["'", relation, "'"]|join).split('.')[2]))|replace("'", ""))|lower }}.*
            --
            from {{ ((((["'", relation, "'"]|join).split('.')[2]))|replace("'", ""))|lower }}
            --
        )   
            
               {%- endif -%}

        {% if not loop.last -%}
        --
            union all
        --
        {% endif -%}

        {%- endfor -%} 

{%- endmacro -%}
