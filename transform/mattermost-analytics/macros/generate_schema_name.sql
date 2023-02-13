{#
    Switch default generate_schema_name with the one the uses a different strategy per environment. You can read
    more here: https://docs.getdbt.com/docs/build/custom-schemas
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ generate_schema_name_for_env(custom_schema_name, node) }}
{%- endmacro %}