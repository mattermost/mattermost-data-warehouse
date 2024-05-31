{% macro load_feature_mappings() %}

    {%- call statement('get_query_results', fetch_result=True) -%}
        select * from {{ ref('tracking_plan') }} order by feature_name
    {%- endcall -%}

    {%- set rules = {} -%}

    {%- if execute -%}
        {{ log('Loading feature mappings') }}

        {%- set rules_result = load_result('get_query_results').table.rows -%}

        {%- for key, group in rules_result | groupby('FEATURE_NAME') -%}
            {% do rules.update({key: [i for i in group]}) %}
        {%- endfor -%}
    {%- endif -%}


    {{ log(rules) }}
    {{ return(rules) }}

{% endmacro %}
