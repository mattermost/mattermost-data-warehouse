{{
    config({
        "materialized": "table",
        "snowflake_warehouse": "transform_l"
    })
}}

{%-
    set values = dbt_utils.get_column_values(ref('int_server_feature_attribution'), 'feature_name')
-%}

{%- if 'unknown_features' not in values -%}
    {%- do values.append('unknown_features') -%}
{%- endif -%}

select
    -- Surrogate key required as it's both a good practice, as well as allows merge incremental strategy.
    {{ dbt_utils.generate_surrogate_key(['activity_date', 'server_id', 'user_id']) }} as daily_user_id
    , activity_date
    , server_id
    , user_id
    , {{ dbt_utils.pivot(
          'feature_name',
          values,
          agg='sum',
          prefix='count_'
      ) }}
    , {% for feature in values if feature != 'unknown_features' %}
         count_{{feature}}  {%- if not loop.last %} + {% endif -%}
    {% endfor %}
    ) as count_known_features
    , count(event_id) as count_total
from
    {{ ref('int_server_feature_attribution') }}
group by
    daily_user_id
    , activity_date
    , server_id
    , user_id
