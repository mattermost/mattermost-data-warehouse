{{
    config({
        "materialized": "table"
    })
}}

{%- set mau_days = 29 -%}
{%- set features = dbt_utils.get_column_values(ref('paid_feature_aliases'), 'alias') -%}

with server_paid_feature_date_range as (
    select
        server_id
        , user_id
        , min(activity_date) as first_active_day
        , max(activity_date) as last_active_day
    from
        {{ ref('int_paid_feature_daily_usage_per_user') }}
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
        server_paid_feature_date_range sd
        left join {{ ref('telemetry_days') }} all_days
            on all_days.date_day >= sd.first_active_day and all_days.date_day <= sd.last_active_day
    -- least(current_date, dateadd(day, sd.last_active_day, {{mau_days}}))
)
select
    spine.server_id
    , spine.user_id
    , spine.activity_date
{% for feature_column in features %}
    {%- set column_name = dbt_utils.slugify('count_feature_' ~ feature_column) -%}

    , coalesce({{ column_name }}, 0) as {{ dbt_utils.slugify(column_name ~ '_daily') }}
    , coalesce (
        sum ({{column_name}}) over(
            partition by spine.server_id, spine.user_id order by spine.activity_date
            rows between {{ mau_days }} preceding and current row
        ), 0
    ) as {{  dbt_utils.slugify(column_name ~ '_monthly') }}
{% endfor %}
from
    spine
    left join {{ ref('int_paid_feature_daily_usage_per_user') }} paid_features
        on spine.server_id = paid_features.server_id and spine.user_id = paid_features.user_id and spine.activity_date = paid_features.activity_date