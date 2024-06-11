{{
    config({
        "materialized": "table",
        "snowflake_warehouse": "transform_l"
    })
}}

{%- set join_columns = ['daily_user_id', 'activity_date', 'server_id', 'user_id'] -%}
{%- set agg_columns = ['count_known_feature', 'count_unknown_feature', 'count_total_events'] -%}
{%- set skip_columns = join_columns + agg_columns + ['received_at_date'] -%}

-- Contains all daily feature usage data per user/server/date and from all sources
select
{% for column in join_columns %}
    {%- if not loop.first %} , {% endif -%} coalesce(m.{{column}}, p.{{column}}) as {{column}}
{% endfor %}
    -- Mattermost features
{% set cols = dbt_utils.get_filtered_columns_in_relation(from=ref('int_feature_daily_usage_per_user'), except=skip_columns) %}
{%- for col in cols %}
    , coalesce(m.{{ adapter.quote(col)|trim }}, 0) as {{ adapter.quote(col)|trim }}
{%- endfor -%}
    -- Playbook features
{% set cols = dbt_utils.get_filtered_columns_in_relation(from=ref('int_playbooks_daily_usage_per_user'), except=skip_columns) %}
{%- for col in cols %}
    , coalesce(p.{{ adapter.quote(col)|trim }}, 0) as {{ adapter.quote(col)|trim }}
{%- endfor -%}
    -- Aggregate columns are joined by summing up the values from all sources
{% for column in agg_columns %}
    , coalesce(m.{{column}}, 0) + coalesce(p.{{column}}, 0) as {{column}}
{% endfor %}
from
    {{ ref('int_feature_daily_usage_per_user') }} m
    full outer join {{ ref('int_playbooks_daily_usage_per_user') }} p
        on  {% for column in join_columns %}
            {%- if not loop.first %} and {% endif -%} m.{{column}} = p.{{column}}
        {% endfor %}