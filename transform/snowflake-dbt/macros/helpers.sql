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