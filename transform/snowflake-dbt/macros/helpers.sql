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
    {% set orgm_tables = ['account', 'campaign', 'campaignmember', 'contact', 'lead', 'opportunity', 'opportunitylineitem', 'territory_mapping__c','task','opportunityfieldhistory'] %}

    {% for t in orgm_tables %}
        {{ delete_orgm_rows(audit_table, t) }}
    {% endfor %}
{% endmacro %}

{% macro delete_orgm_rows(audit_table, orgm_table) %}
    {% set query %}
        delete from {{ source('orgm_old', orgm_table) }}
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

{% macro license_cleaning(destination_table) %}
    {% set query %}
        delete from analytics.blp.license_server_fact using (SELECT license_id FROM analytics.blp.license_server_fact 
        WHERE server_id IS NOT NULL GROUP BY 1) lsf
        WHERE license_server_fact.license_id = lsf.license_id AND license_server_fact.server_id is null
    {% endset %}

    {% do run_query(query) %}
{% endmacro %}

{% macro get_rudder_track_tables(schema, database=target.database, table_exclusions=table_exclusions, table_inclusions=table_inclusions) %}
    {% for scheme in schema %}
    select distinct
        table_schema as "table_schema", table_name as "table_name"
    from {{database}}.information_schema.tables
    WHERE table_schema ilike '{{ scheme }}'
        {% if table_inclusions != "'pages'" %}
        AND table_name not in ('TRACKS', 'USERS', 'SCREENS', 'IDENTIFIES', 'PAGES', 'RUDDER_DISCARDS')
        {% elif scheme == 'mm_telemetry_rc' or scheme == 'mm_telemetry_qa' %}
        AND table_name not in ('TRACKS', 'USERS', 'SCREENS', 'IDENTIFIES', 'PAGES', 'RUDDER_DISCARDS')
        AND REGEXP_SUBSTR(TABLE_NAME, '[0-9]') IS NULL
        AND table_name not like 'EVENT_%'
        {% endif %}
        {% if this.table == 'server_telemetry' %}
        AND table_name not in ('TRACKS', 'USERS', 'SCREENS', 'IDENTIFIES', 'PAGES', 'RUDDER_DISCARDS')
        AND REGEXP_SUBSTR(TABLE_NAME, '[0-9]') IS NULL
        AND table_name not like 'EVENT_%'
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

{% macro get_target_table(schema = this.schema, database=this.database) %}
    select distinct
        table_schema as "table_schema", table_name as "table_name"
    from {{this.database}}.information_schema.tables
    WHERE table_schema ilike '{{ this.schema }}'
    AND table_name ilike '{{ this.table }}'

{% endmacro %}

{%- macro add_new_columns(relation, column_superset, source_column_name=none, column_override=none) -%}

    {#-- Prevent querying of db in parsing mode. This works because this macro does not create any new refs. -#}
    {%- if not execute %}
        {{ return('') }}
    {% endif -%}

    {%- set column_override = column_override if column_override is not none else {} -%}
    {%- set source_column_name = source_column_name if source_column_name is not none else '_dbt_source_relation' -%}
    {%- set dtypes = {} -%} 
    {%- set match_names = [] -%}

    {%- set missing_columns = {} -%}
    {%- for tbl in relation -%}
        {%- do dbt_utils._is_relation(tbl, 'add_new_columns') -%}
        {%- set cols = adapter.get_columns_in_relation(tbl) -%}
        {%- for key, value in column_superset.items() -%}
        
            {%- if value not in cols -%}
                {%- for col in cols -%}
                    {%- if (value.name == col.name) and (value.data_type != col.data_type) -%}

                        {%- do dtypes.update({col.column: col.data_type}) -%}
                        {%- do match_names.append(col.column) -%}
                
                    {%- elif value.name == col.name -%}

                        {%- do dtypes.update({col.column: col.data_type}) -%}
                        {%- do match_names.append(col.name) -%}
                    
                    {%- endif -%}
                {%- endfor -%}

                {%- if value.name not in match_names -%}
                    {%- do missing_columns.update({value.column: value}) -%}
                {%- endif -%}

            {%- endif -%}

        {%- endfor -%}
    {%- endfor -%}

    {%- set missing_column_names = missing_columns.keys() -%}

    {%- if missing_columns == {} -%}
        SELECT CURRENT_TIMESTAMP
    {%- else -%}

    ALTER TABLE {{ this.database }}.{{ this.schema }}.{{ this.table }} ADD COLUMN
    {% for col_name in missing_column_names -%}
        {%- if col_name not in dtypes.keys() -%}

        {%- set col = missing_columns[col_name] -%}
        {%- set col_type = column_override.get(col.column, col.data_type) -%}
        {%- set col_name = col_name | lower -%}

            {{ col_name }} {{ col_type }}

            {%- if not loop.last -%},{%- elif loop.last -%};
            {%- endif -%}
        {%- endif -%}

    {%- endfor -%}
    {%- endif -%}

{% endmacro %}

{% macro get_rudder_track_column_count(schema=this.schema, database=this.database, table_exclusions=table_exclusions, table_inclusions=table_inclusions) %}
    {%- call statement('column_count', fetch_result=True) -%}
        select distinct
            MAX(ordinal_position) as "column_count"
        from {{database}}.information_schema.columns
        WHERE table_schema ilike '{{ this.schema }}'
        AND table_name ilike '{{ this.table }}'
    {%- endcall -%}

    {% if execute %}
        {%- set result = load_result('column_count').table.columns['column_count'].values()[0] | int -%}
        {{ return(result)}}
    {% else %}
        {{return(none)}}
    {% endif %}

{% endmacro %}

{% macro get_rudder_relations(schema, database=target.database, table_exclusions="", table_inclusions="") %}

    {%- call statement('get_tables', fetch_result=True) %}

      {{ get_rudder_track_tables(schema, database, table_exclusions=table_exclusions, table_inclusions=table_inclusions) }}

    {%- endcall -%}

    {%- call statement('get_target_table', fetch_result=True) %}

      {{ get_target_table(schema, database) }}

    {%- endcall -%}

    {%- set table_list = load_result('get_tables') -%}
    {%- set target_table = load_result('get_target_table') -%}

    {%- if table_list and table_list['table'] -%}
        {%- set tbl_relations = [] -%}
        {%- for row in table_list['table'] -%}
            {%- set tbl_relation = api.Relation.create(database, row.table_schema, row.table_name) -%}
            {%- do tbl_relations.append(tbl_relation) -%}
        {%- endfor -%}

        {%- set tgt_tbl_relations = [] -%}
        {%- for row in target_table['table'] -%}
            {%- set tgt_tbl_relation = api.Relation.create(this.database, row.table_schema, row.table_name) -%}
            {%- do tgt_tbl_relations.append(tgt_tbl_relation) -%}
        {%- endfor -%}

        {{ return([tbl_relations, tgt_tbl_relations]) }}
    {%- else -%}
        {{ return([]) }}
    {%- endif -%}

{% endmacro %}

{% macro union_relations(relations, tgt_relation, column_override=none, include=[], exclude=[], source_column_name=none) %}

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
    
    {%- call statement('add_new_columns', fetch_result=True) %}

      {{ add_new_columns(relation = tgt_relation, column_superset=column_superset) }}

    {%- endcall -%}

    {%+ if is_incremental() %}
        with
        {%+ if this.table == 'user_events_telemetry' %}
        max_time AS (
                 SELECT
                    _dbt_source_relation2
                    , MAX(received_at) as max_time
                 FROM {{ this }} 
                 WHERE {{this}}.received_at <= CURRENT_TIMESTAMP
                 GROUP BY 1
             ),
        {%+ elif this.table == 'performance_events' %}
                max_time AS (
                 SELECT
                    _dbt_source_relation
                    , MAX(received_at) as max_time
                 FROM {{ this }} 
                 WHERE {{this}}.received_at <= CURRENT_TIMESTAMP
                 GROUP BY 1
             ),
             {%+ else %}
             max_time AS (
                 SELECT 
                    MAX(received_at) AS max_time
                 FROM {{ this }} 
                 WHERE received_at <= CURRENT_TIMESTAMP
             ),
            {%+ endif %}

    {%+ endif %}

    {%+ if not is_incremental() %}
    with 
    {%+ endif %}
    {% for relation in relations %}
               {%+ if this.table == 'daily_website_traffic' %}
               {{ ((((["'", relation, "'"]|join).split('.')[1]))|replace("'", ""))|lower }}_{{ ((((["'", relation, "'"]|join).split('.')[2]))|replace("'", ""))|lower }} AS (
               {%+ else %}
               {{ ((((["'", relation, "'"]|join).split('.')[2]))|replace("'", ""))|lower }} AS (
               {%+ endif %}   
               select
                cast({{ dbt_utils.string_literal(relation) }} as {{ dbt_utils.type_string() }}) as {{ source_column_name if ('_DBT_SOURCE_RELATION' not in column_superset) else '_DBT_SOURCE_RELATION2'}},
                {%+ for col_name in ordered_column_names %}
                    {%- set col = column_superset[col_name] -%}
                    {%- set col_type = column_override.get(col.column, col.data_type) -%}
                    {%- set col_name = adapter.quote(col_name) if col_name in relation_columns[relation] else 'null' -%}
                        {%- if col.quoted[-10:-1] == 'TIMESTAMP' and col.quoted[1:3] == 'NPS' -%}
                        cast({{ relation }}.ORIGINAL_TIMESTAMP as DATE) as {{ col.quoted }}
                        {%- else -%}
                        cast({{ col_name }} as {{ col_type }}) as {{ col.quoted }}
                        {%- endif -%}
                        {% if not loop.last %}, {% elif loop.last %}
                        {% endif %}
                {%+ endfor %}   
              from 
              {% if (((relation|replace("'", "")|join).split('.')[0]))|lower == '"raw"' -%}
              {{ relation }}
              {% else -%}
              {{ ref((((["'", relation|replace("'", "")]|join).split('.')[2]))|lower) }}
              {%- endif -%}
              {%+ if is_incremental() and this.table == 'user_events_telemetry' %}
                JOIN max_time mt
                    ON {{ relation }}.received_at > mt.max_time
                    AND mt._dbt_source_relation2 = {{ ["'", relation, "'"]|join }}
                WHERE received_at <= CURRENT_TIMESTAMP
             {%+ elif is_incremental() and this.table == 'performance_events' %}
                JOIN max_time mt
                    ON {{ relation }}.received_at > mt.max_time
                    AND mt._dbt_source_relation = {{ ["'", relation, "'"]|join }}
                WHERE received_at <= CURRENT_TIMESTAMP
            {%+ elif is_incremental() %}
                JOIN max_time mt
                    ON {{ relation }}.received_at > mt.max_time
                WHERE received_at <= CURRENT_TIMESTAMP
            {%+ endif %}
            {%- if this.table in ['user_events_telemetry', 'rudder_webapp_events', 'mobile_events', 'portal_events', 'cloud_pageview_events', 'cloud_portal_pageview_events'] -%}
                {%+ if is_incremental() %}
                AND 
                {%+ else %}
                WHERE 
                {%+ endif %}
                {%+ if 'EVENT' in relation_columns[relation] and 'TYPE' in relation_columns[relation] %}
                    COALESCE({{ relation }}.type, {{ relation }}.event) NOT IN (
                {%+ elif 'EVENT' not in relation_columns[relation] and 'TYPE' in relation_columns[relation] %}
                    {{ relation }}.type NOT IN (
                {%+ elif 'EVENT' in relation_columns[relation] and 'TYPE' not in relation_columns[relation] %}
                    {{ relation }}.event NOT IN (
                {%+ endif %}
                                            'api_channel_get',
                                            'api_channel_get_by_name_and_teamname',
                                            'api_channels_join_direct',
                                            'api_posts_get_after',
                                            'api_posts_get_before',
                                            'api_profiles_get',
                                            'api_profiles_get_by_ids',
                                            'api_profiles_get_by_usernames',
                                            'api_profiles_get_in_channel',
                                            'api_profiles_get_in_group_channels',
                                            'api_profiles_get_in_team',
                                            'api_profiles_get_not_in_channel',
                                            'api_profiles_get_not_in_team',
                                            'api_profiles_get_without_team',
                                            'channel_switch',
                                            'page_load',
                                            'lhs_dm_gm_count',
                                            'ui_channel_selected',
                                            'ui_channel_selected_v2',
                                            'ui_direct_channel_x_button_clicked',
                                            'application_backgrounded',
                                            'application_installed',
                                            'application_opened',
                                            'application_updated')
                AND {{ relation }}.TIMESTAMP::DATE <= CURRENT_DATE                            
            {% elif this.table in ['performance_events'] %}
                {%+ if is_incremental() %}
                AND 
                {%+ else %}
                WHERE 
                {%+ endif %}
                {{ relation }}.category IN ('performance') 
                {%+ if (((relation|replace("'", "")|join).split('.')[1]))|lower == 'mm_telemetry_rc' %}
                    AND user_id = '93mykbogbjfrbbdqphx3zhze5c' 
                {%+ endif %}   
            {% endif %}
            ){% if not loop.last %}, 
            {% endif %}
    {%- endfor -%}

        {%- for relation in relations -%}
            {% if this.table == 'daily_website_traffic' %}
            (
                select
                {{ ((((["'", relation, "'"]|join).split('.')[1]))|replace("'", ""))|lower }}_{{ ((((["'", relation, "'"]|join).split('.')[2]))|replace("'", ""))|lower }}.*
                from {{ ((((["'", relation, "'"]|join).split('.')[1]))|replace("'", ""))|lower }}_{{ ((((["'", relation, "'"]|join).split('.')[2]))|replace("'", ""))|lower }} 
            )
            {% else %}
            (
                select
                    {{ ((((["'", relation, "'"]|join).split('.')[2]))|replace("'", ""))|lower }}.*
                from {{ ((((["'", relation, "'"]|join).split('.')[2]))|replace("'", ""))|lower }}
            )   
            {% endif %}
            {% if not loop.last %}
            union all
            {% endif %}
        {% endfor %} 
{% endmacro %}

{% macro latest_record(src) %}
    SELECT DISTINCT o.id as sfid, o.*
    FROM {{ src }} o
    INNER JOIN (
        SELECT id,
                MAX(_sdc_sequence) AS seq,
                MAX(_sdc_batched_at) AS batch
        FROM {{ src }}
        GROUP BY id) oo
    ON o.id = oo.id
        AND o._sdc_sequence = oo.seq
        AND o._sdc_batched_at = oo.batch
{%- endmacro %}
