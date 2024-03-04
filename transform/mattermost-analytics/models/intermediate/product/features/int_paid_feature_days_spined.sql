{{
    config({
        "materialized": "table",
    })
}}
-- TODO: make incremental

{%- set mau_days = 29 -%}
{%- set features = dbt_utils.get_column_values(ref('paid_feature_aliases'), 'alias') -%}

with server_paid_feature_date_range as (
    select
        server_id
        , user_id
        , min(server_date) as first_server_date
        , max(server_date) as last_server_date
    from
        {{ ref('int_paid_feature_daily_usage_per_user') }}
    where
        server_date >= '{{ var('telemetry_start_date')}}'
    group by
        server_id
), spine as (
    select
        sd.server_id
        , sd.user_id
        , all_days.date_day::date as activity_date
    from
        server_paid_feature_date_range sd
        left join {{ ref('telemetry_days') }} all_days
            on all_days.date_day >= sd.first_active_day and all_days.date_day <= least(current_date, dateadd(day, sd.last_active_day, {{mau_days}}))
)
select
    daily_server_id
    , server_id
    , activity_date
{% for feature_column in features %}
    {%- set column_name = adapter.quote('feature_' ~ feature_column) -%}

    , {{ column_name }} as {{  adapter.quote(column_name ~ '_daily') }}
    , max({{column_name}}) over(
        partition by spine.server_id, spine.user_id order by spined.date_day
        rows between {{ mau_days }} preceding and current row
    ) as {{  adapter.quote(column_name ~ '_monthly') }}
{% endfor %}

from
    spine
    left join {{ ref('int_paid_feature_daily_usage_per_user') }} paid_features