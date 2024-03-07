{%- set metric_cols = dbt_utils.get_filtered_columns_in_relation(ref('int_feature_daily_spine'), except=['server_id', 'user_id', 'activity_date']) -%}

with server_date_range as (
    select
        server_id
        , min(activity_date) as first_active_day
        , max(activity_date) as last_active_day
    from
        {{ ref('int_feature_daily_spine') }}
    where
        activity_date >= '{{ var('telemetry_start_date')}}'
    group by
        server_id
), server_spine as (
    select
        sd.server_id
        , all_days.date_day::date as activity_date
        , {{ dbt_utils.generate_surrogate_key(['server_id', 'activity_date']) }} AS daily_server_id
    from
        server_date_range sd
        left join {{ ref('telemetry_days') }} all_days
            on all_days.date_day >= sd.first_active_day and all_days.date_day <= sd.last_active_day
)
select
    server_spine.daily_server_id
    , server_spine.server_id
    , server_spine.activity_date
{% for metric_column in metric_cols -%}
    , coalesce(sum({{metric_column}}), 0) as {{metric_column}}
{%- endfor %}
from
    server_spine
    left join {{ ref('int_feature_daily_spine') }} feature_spine
        on server_spine.server_id = feature_spine.server_id and server_spine.activity_date = feature_spine.activity_date
group by
    server_spine.daily_server_id
    , server_spine.server_id
    , server_spine.activity_date