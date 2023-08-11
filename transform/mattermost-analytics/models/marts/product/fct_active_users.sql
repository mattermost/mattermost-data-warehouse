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
    m.*,
    -- Server activity
    coalesce(sas.daily_active_users, 0) as server_daily_active_users,
    coalesce(sas.monthly_active_users, 0) as server_monthly_active_users,
    coalesce(sas.count_registered_users, 0) as count_registered_users,
    coalesce(sas.count_registered_deactivated_users, 0) as count_registered_deactivated_users,
    coalesce(sas.is_missing_activity_data, true) as is_missing_server_activity_data
from
    metrics m
    left join {{ ref('int_server_activity_spined')}} sas on m.daily_server_id = sas.daily_server_id
where
    m.server_id not in (
        select server_id from {{ ref('dim_excludable_servers') }}
    )
