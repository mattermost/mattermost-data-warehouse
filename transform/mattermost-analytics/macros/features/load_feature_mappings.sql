{% macro load_feature_mappings() %}

    {%- call statement('rules', fetch_result=True) -%}
        select * from {{ ref('tracking_plan') }} order by feature_name
    {%- endcall -%}


    {% set itertools = modules.itertools %}
    {%- set raw_rules = load_result('rules') -%}

    {%- set rules = modules.itertools.groupby(raw_rules.table.rows, key=lambda r: r['FEATURE_NAME']) -%}

    {{ log(rules) }}

    {{ return(rules) }}

{% endmacro %}
