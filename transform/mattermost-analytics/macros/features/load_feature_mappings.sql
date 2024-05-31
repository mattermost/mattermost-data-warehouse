{% macro load_feature_mappings() %}

    {%- call statement('rules', fetch_result=True) -%}
        select * from {{ ref('tracking_plan') }} order by feature_name
    {%- endcall -%}


    {% set itertools = modules.itertools %}
    {%- set result = load_result('rules') -%}


    {%- set rules = dict([ (k, list(v)) for k, v in  modules.itertools.groupby(result.table.rows, key=lambda x: x['FEATURE_NAME'])]) -%}

    {{ log(rules) }}

    {{ return(rules) }}

{% endmacro %}
