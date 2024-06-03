-- Materialization required in order for downstream model to be able to get list of columns.
{{
    config({
        "materialized": "table",
        "snowflake_warehouse": "transform_l"
    })
}}

{%
    set count_columns = dbt_utils.get_filtered_columns_in_relation(
        from=ref('int_feature_daily_usage_pivoted'),
        except=["daily_user_id", "activity_date", "server_id", "user_id", "received_at_date"]
    )
%}

{%
    set count_feature_columns = dbt_utils.get_filtered_columns_in_relation(
        from=ref('int_feature_daily_usage_pivoted'),
        except=["daily_user_id", "activity_date", "server_id", "user_id", "received_at_date", "count_unknown"]
    )
%}


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
    -- Spine fields
    spine.server_id
    , spine.user_id
    , spine.activity_date

    -- Aggregation per known feature
{% for column in count_columns %}
    -- For each feature, count the number of of daily and monthly events
    , coalesce({{ column }}, 0) as {{ dbt_utils.slugify(column ~ '_events_daily') }}
    , coalesce (
        sum ({{column}}) over(
            partition by spine.server_id, spine.user_id order by spine.activity_date
            rows between {{var('monthly_days')}} preceding and current row
        ), 0
    ) as {{ dbt_utils.slugify(column ~ '_events_monthly') }}
    -- For each feature, flag the user for daily and monthly
    , iff(coalesce({{ column }}, 0) > 0, 1, 0) as {{ dbt_utils.slugify(column ~ '_users_daily') }}
    , iff(
        coalesce (
            sum ({{column}}) over(
                partition by spine.server_id, spine.user_id order by spine.activity_date
                rows between {{var('monthly_days')}} preceding and current row
            ), 0
        ) > 0, 1, 0
    ) as {{ dbt_utils.slugify(column ~ '_users_monthly') }}
{% endfor %}

    -- Aggregation for known features. To be used for comparing users using any paid feature.
    , {% for column in count_feature_columns -%}
        {{ dbt_utils.slugify(column ~ '_events_daily') }} {%- if not loop.last %} + {% endif -%}
    {%- endfor %} as count_known_feature_events_daily
    , {% for column in count_feature_columns  -%}
        {{ dbt_utils.slugify(column ~ '_events_monthly') }} {%- if not loop.last -%} + {%- endif -%}
    {%- endfor %} as count_known_feature_events_monthly
    , iff(count_known_feature_events_daily > 0, 1, 0) as count_known_feature_users_daily
    , iff(count_known_feature_events_monthly > 0, 1, 0) as count_known_feature_users_monthly
    -- DAU/MAU
    , iff(features.server_id is null, 0, 1) as is_active
    , max(is_active) over(
                partition by spine.server_id, spine.user_id order by spine.activity_date
                rows between {{var('monthly_days')}} preceding and current row
    ) as is_active_monthly
from
    spine
    left join {{ ref('int_feature_daily_usage_pivoted') }} features
        on spine.server_id = features.server_id and spine.user_id = features.user_id and spine.activity_date = features.activity_date