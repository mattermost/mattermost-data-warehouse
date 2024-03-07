-- Materialization required in order for downstream model to be able to get list of columns.
{{
    config({
        "materialized": "table",
    })
}}

{# List of known features #}
{%- set features = dbt_utils.get_column_values(ref('feature_aliases'), 'alias') -%}

with server_feature_date_range as (
    select
        server_id
        , user_id
        , min(activity_date) as first_active_day
        , max(activity_date) as last_active_day
    from
        {{ ref('int_feature_daily_usage_pivoted') }}
    where
        activity_date >= '{{ var('telemetry_start_date')}}'
    group by
        server_id, user_id
), spine as (
    select
        sd.server_id
        , sd.user_id
        , all_days.date_day::date as activity_date
    from
        server_feature_date_range sd
        left join {{ ref('telemetry_days') }} all_days
            on all_days.date_day >= sd.first_active_day and all_days.date_day <= least(current_date, dateadd(day, {{var('monthly_days')}}, sd.last_active_day))
)
select
    spine.server_id
    , spine.user_id
    , spine.activity_date
{% for feature_column in features %}
    -- For each feature, count the number of of daily and monthly events
    , coalesce({{ feature_column }}, 0) as {{ dbt_utils.slugify('count_' ~ feature_column ~ '_events_daily') }}
    , coalesce (
        sum ({{feature_column}}) over(
            partition by spine.server_id, spine.user_id order by spine.activity_date
            rows between {{var('monthly_days')}} preceding and current row
        ), 0
    ) as {{ dbt_utils.slugify('count_' ~ feature_column ~ '_events_monthly') }}
    -- For each feature, flag the user for daily and monthly
    , iff(coalesce({{ feature_column }}, 0) > 0, 1, 0) as {{ dbt_utils.slugify('count_' ~ feature_column ~ '_users_daily') }}
    , iff(
        coalesce (
            sum ({{feature_column}}) over(
                partition by spine.server_id, spine.user_id order by spine.activity_date
                rows between {{var('monthly_days')}} preceding and current row
            ), 0
        ) > 0, 1, 0
    ) as {{ dbt_utils.slugify('count_' ~ feature_column ~ '_users_monthly') }}
{% endfor %}
from
    spine
    left join {{ ref('int_feature_daily_usage_pivoted') }} features
        on spine.server_id = features.server_id and spine.user_id = features.user_id and spine.activity_date = features.activity_date