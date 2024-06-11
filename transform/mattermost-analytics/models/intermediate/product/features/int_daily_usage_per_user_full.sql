-- Contains all daily feature usage data per user/server/date and from all sources
{%- set common_columns = ['daily_user_id', 'activity_date', 'server_id', 'user_id', 'received_at_date'] -%}
{%- set agg_columns = ['count_known_feature', 'count_unknown_feature', 'count_total_events'] -%}

select
{% for column in common_columns %}
    {%- if not loop.first %} , {% endif -%} coalesce(m.{{column}}, p.{{column}}) as {{column}}
{% endfor %}
    -- Mattermost features
    {% set cols = dbt_utils.get_filtered_columns_in_relation(ref('int_feature_daily_usage_per_user'), common_columns + agg_columns) %}
    {%- for col in cols %}
        , {% endif -%} coalesce(m.{{column}}, 0) as {{column}}
    {%- endfor -%}
    -- Playbook features
    {% set cols = dbt_utils.get_filtered_columns_in_relation(ref('int_playbooks_daily_usage_per_user'), common_columns + agg_columns) %}
    {%- for col in cols %}
        , {% endif -%} coalesce(p.{{column}}, 0) as {{column}}
    {%- endfor -%}
    , {{ dbt_utils.star(from=ref('int_feature_daily_usage_per_user'), except=common_columns + agg_columns, relation_alias='m') }}
    , {{ dbt_utils.star(from=ref('int_playbooks_daily_usage_per_user'), except=common_columns + agg_columns, relation_alias='p') }}
    -- Aggregate columns are joined by summing up the values from all sources
{% for column in agg_columns %}
    , coalesce(m.{{column}}, 0) + coalesce(p.{{column}}, 0) as {{column}}
{% endfor %}
from
    {{ ref('int_feature_daily_usage_per_user') }} m
    full outer join {{ ref('int_playbooks_daily_usage_per_user') }} p
        on  {% for column in common_columns %}
            {%- if not loop.first %} and {% endif -%} m.{{column}} = p.{{column}})
        {% endfor %}