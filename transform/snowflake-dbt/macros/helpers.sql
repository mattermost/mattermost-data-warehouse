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