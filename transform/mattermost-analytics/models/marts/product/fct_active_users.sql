{%
    set column_map = {
        'is_active': 'active_users',
        'is_client_desktop': 'client_desktop_active_users',
        'is_client_webapp': 'client_webapp_active_users',
        'is_legacy_desktop': 'legacy_desktop_active_users',
        'is_legacy_webapp': 'legacy_webapp_active_users',
        'is_desktop': 'desktop_active_users',
        'is_webapp': 'webapp_active_users',
        'is_unknown': 'unknown_active_users'
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
    coalesce(m.daily_server_id, sas.daily_server_id) as daily_server_id
    , coalesce(m.activity_date, sas.activity_date) as activity_date
    , coalesce(m.server_id, sas.server_id) as server_id
    {% for metric, column in column_map.items() %}
    , coalesce(m.daily_{{column}}, 0) as daily_{{column}}
    , coalesce(m.weekly_{{column}}, 0) as weekly_{{column}}
    , coalesce(m.monthly_{{column}}, 0) as monthly_{{column}}
    {% endfor %}
    -- Server-reported activity
    , coalesce(sas.daily_active_users, 0) as server_daily_active_users
    , coalesce(sas.monthly_active_users, 0) as server_monthly_active_users
    , coalesce(sas.count_registered_users, 0) as count_registered_users
    , coalesce(sas.count_registered_deactivated_users, 0) as count_registered_deactivated_users
    , coalesce(sas.count_public_channels, 0) as count_public_channels
    , coalesce(sas.count_private_channels, 0) as count_private_channels
    , coalesce(sas.count_teams, 0) as count_teams
    , coalesce(sas.count_slash_commands, 0) as count_slash_commands
    , coalesce(sas.count_direct_message_channels, 0) as count_direct_message_channels
    , coalesce(sas.count_posts, 0) as count_posts
    -- Extra dimensions
    , {{ dbt_utils.generate_surrogate_key(['sas.version_full']) }} AS version_id
    -- Metadata
    , coalesce(sas.is_missing_activity_data, true) as is_missing_server_activity_data
    , m.server_id is not null as has_user_telemetry_data
    , sas.server_id is not null as has_server_telemetry_data

from
    metrics m
    -- Use full outer as there might be servers without front-end telemetry
    full outer join {{ ref('int_server_active_days_spined')}} sas on m.daily_server_id = sas.daily_server_id

