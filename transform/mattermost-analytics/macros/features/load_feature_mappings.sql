{% macro load_feature_mappings() %}

    {%- call statement('rules', fetch_result=True) -%}
        select * from {{ ref('tracking_plan') }} order by feature_name
    {%- endcall -%}


    {% set itertools = modules.itertools %}
    {%- set result = load_result('rules') -%}

    {%- set rules = {} -%}
    {%- set group_key = lambda row: row['FEATURE_NAME'] -%}

    {%- for key, group in  modules.itertools.groupby(result.table.rows, key=group_key) -%}
        {% do rules.update({key: list(group)}) %}
    {%- endfor -%}



    {{ log(rules) }}

    {{ return(rules) }}

{% endmacro %}
