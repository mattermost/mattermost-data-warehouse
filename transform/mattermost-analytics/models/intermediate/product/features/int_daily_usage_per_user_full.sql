{{
    config({
        "materialized": "table",
        "snowflake_warehouse": "transform_l"
    })
}}

{%- set join_columns = ['daily_user_id', 'activity_date', 'server_id', 'user_id'] -%}
{%- set agg_columns = ['count_known_features', 'count_unknown_features', 'count_total'] -%}
{%- set skip_columns = join_columns + agg_columns + ['received_at_date'] -%}

-- List of models and their aliases. that include partial daily feature usage.
-- Add new models here.
{%-
set partial_daily_models = {
    ref('int_feature_daily_usage_per_user'): "mattermost",
    ref('int_playbooks_daily_usage_per_user'): "playbooks",
    ref('int_calls_daily_usage_per_user'): "calls",
    ref('int_copilot_daily_usage_per_user'): "copilot",
}
-%}

-- Create a list of dates for each user/server where the user shows some "presence".
-- Results in a spine of the output dates, helping avoid full outer joins.
with user_daily_presence as (
{% for model in partial_daily_models.keys() %}

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
    {%- if not loop.first %} , {% endif -%} spine.{{column}}
{% endfor %}
    -- Features from individual models
{% for model, alias in partial_daily_models.items() %}
    -- {{ alias }} features
    {% set cols = dbt_utils.get_filtered_columns_in_relation(from=model, except=skip_columns) %}
    {% for col in cols %}
        , coalesce({{alias}}.{{ adapter.quote(col)|trim }}, 0) as {{ adapter.quote(col)|trim }}
    {% endfor %}
{% endfor %}

    -- Aggregate columns are joined by summing up the values from all sources
{% for column in agg_columns %}
    , {% for alias in partial_daily_models.values() %}
    {%- if not loop.first -%} + {% endif -%} coalesce({{alias}}.{{column}}, 0)
    {% endfor %}
       as {{column}}
{% endfor %}


from
    user_daily_presence spine
{% for model, alias in partial_daily_models.items() %}
    left join {{ model }} {{ alias }}
        on  {% for column in join_columns %}
            {%- if not loop.first %} and {% endif -%} spine.{{column}} = {{alias}}.{{column}}
        {% endfor %}
{% endfor %}

