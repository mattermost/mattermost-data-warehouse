{{
    config({
        "materialized": "table",
        "snowflake_warehouse": "transform_l"
    })
}}

{%- set join_columns = ['daily_user_id', 'activity_date', 'server_id', 'user_id'] -%}
{%- set agg_columns = ['count_known_features', 'count_unknown_features', 'count_total'] -%}
{%- set skip_columns = join_columns + agg_columns + ['received_at_date'] -%}

-- List of models that include partial daily feature usage.
{%-
set partial_daily_models = [
    ref('int_feature_daily_usage_per_user'),
    ref('int_playbooks_daily_usage_per_user'),
    ref('int_calls_daily_usage_per_user')
]
-%}

-- Create a list of dates for each user/server where the user shows some "presence".
-- Results in a spine of the output dates, helping avoid full outer joins.
with user_daily_presence as (
{% for model in partial_daily_models %}

    select
    {% for column in join_columns %}
        {%- if not loop.first %} , {% endif -%}{{column}}
    {% endfor %}
    from
        {{ model }}

    {% if not loop.last %}union{% endif -%}

{% endfor %}
)
-- Contains all daily feature usage data per user/server/date and from all sources
select
{% for column in join_columns %}
    {%- if not loop.first %} , {% endif -%} spine.{{column}})
{% endfor %}
    -- Mattermost features
{% set cols = dbt_utils.get_filtered_columns_in_relation(from=ref('int_feature_daily_usage_per_user'), except=skip_columns) %}
{% for col in cols %}
    , coalesce(m.{{ adapter.quote(col)|trim }}, 0) as {{ adapter.quote(col)|trim }}
{% endfor %}
    -- Playbook features
{% set cols = dbt_utils.get_filtered_columns_in_relation(from=ref('int_playbooks_daily_usage_per_user'), except=skip_columns) %}
{% for col in cols %}
    , coalesce(p.{{ adapter.quote(col)|trim }}, 0) as {{ adapter.quote(col)|trim }}
{% endfor %}
    -- Calls features
{% set cols = dbt_utils.get_filtered_columns_in_relation(from=ref('int_calls_daily_usage_per_user'), except=skip_columns) %}
{% for col in cols %}
    , coalesce(c.{{ adapter.quote(col)|trim }}, 0) as {{ adapter.quote(col)|trim }}
{% endfor %}
    -- Aggregate columns are joined by summing up the values from all sources
{% for column in agg_columns %}
    , coalesce(m.{{column}}, 0) + coalesce(p.{{column}}, 0) + coalesce(c.{{column}}, 0) as {{column}}
{% endfor %}
from
    user_daily_presence spine
    left join {{ ref('int_feature_daily_usage_per_user') }} m
        on  {% for column in join_columns %}
            {%- if not loop.first %} and {% endif -%} spine.{{column}} = m.{{column}}
        {% endfor %}
    left join {{ ref('int_playbooks_daily_usage_per_user') }} p
        on  {% for column in join_columns %}
            {%- if not loop.first %} and {% endif -%} spine.{{column}} = p.{{column}}
        {% endfor %}
    left join {{ ref('int_calls_daily_usage_per_user') }} c
        on  {% for column in join_columns %}
            {%- if not loop.first %} and {% endif -%} spine.{{column}} = c.{{column}}
        {% endfor %}
