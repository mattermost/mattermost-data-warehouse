{%
    set column_map = {
        'is_active': 'active_users',
        'is_desktop_or_server': 'desktop_active_users',
        'is_mobile': 'mobile_active_users',
        'is_old_server': 'legacy_active_users'
    }
%}

select
    {{ dbt_utils.generate_surrogate_key(['activity_date', 'server_id']) }} AS id
    , activity_date
    , server_id
{% for metric, column in column_map.items() %}
    , sum({{metric}}_today::integer) as daily_{{column}}
    , sum({{metric}}_last_7_days::integer) as weekly_{{column}}
    , sum({{metric}}_last_30_days::integer) as monthly_{{column}}
{% endfor %}
from {{ ref('int_user_active_days_spined') }}
group by activity_date, server_id
