{%
    set column_map = {
        'is_active': 'active_users',
        'is_desktop_or_server': 'desktop_active_users',
        'is_mobile': 'mobile_active_users',
        'is_old_server': 'legacy_active_users'
    }
%}

with metrics as (
    select
        {{ dbt_utils.generate_surrogate_key(['server_id', 'activity_date']) }} AS daily_server_id
        , activity_date
        , server_id
    {% for metric, column in column_map.items() %}
        , sum({{metric}}_today::integer) as daily_{{column}}
        , sum({{metric}}_last_7_days::integer) as weekly_{{column}}
        , sum({{metric}}_last_30_days::integer) as monthly_{{column}}
    {% endfor %}
    from {{ ref('int_user_active_days_spined') }}
    group by activity_date, server_id
)
select
    -- Client telemetry
    m.daily_server_id
    , m.activity_date
    , m.server_id
    {% for metric, column in column_map.items() %}
    , coalesce(daily_{{column}}, 0) as daily_{{column}}
    , coalesce(weekly_{{column}}, 0) as weekly_{{column}}
    , coalesce(monthly_{{column}}, 0) as monthly_{{column}}
    {% endfor %}
    -- Server-reported activity
    , coalesce(sas.daily_active_users, 0) as server_daily_active_users
    , coalesce(sas.monthly_active_users, 0) as server_monthly_active_users
    , coalesce(sas.count_registered_users, 0) as count_registered_users
    , coalesce(sas.count_registered_deactivated_users, 0) as count_registered_deactivated_users
    , coalesce(sas.is_missing_activity_data, true) as is_missing_server_activity_data
    -- Extra dimensions
    , {{ dbt_utils.generate_surrogate_key(['sas.version_full']) }} AS version_id

from
    metrics m
    -- Use full outer as there might be servers without front-end telemetry
    full outer join {{ ref('int_server_active_days_spined')}} sas on m.daily_server_id = sas.daily_server_id

