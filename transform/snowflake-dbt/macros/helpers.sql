{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    {%- set prefix = target.name if target.name != 'prod' else '' -%}

    {%- if custom_schema_name is none -%}

        {{prefix}}_{{ default_schema }}

    {%- else -%}

        {{prefix}}_{{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}