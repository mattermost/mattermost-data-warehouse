{% macro load_feature_mappings() %}

    {%- call statement('rules', fetch_result=True) -%}
        select * from {{ ref('tracking_plan') }} order by feature_name
    {%- endcall -%}

    {%- set rules_result = load_result('rules') -%}

    {%- set rules = {} -%}

    {%- for key, group in rules_result.table.rows | groupby('FEATURE_NAME') -%}
        {% do rules.update({key: list(group)}) %}
    {%- endfor -%}



    {{ log(rules) }}

    {{ return(rules) }}

{% endmacro %}
