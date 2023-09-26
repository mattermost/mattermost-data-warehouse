select
    {{ dbt_utils.generate_surrogate_key(['server_id', 'activity_date']) }} AS daily_server_id
    , activity_date
    , server_id
    , sum(is_active_today::integer) as daily_active_users
    , sum(is_active_last_7_days::integer) as weekly_active_users
    , sum(is_active_last_30_days::integer) as monthly_active_users
from {{ ref('int_boards_active_days_spined') }}
group by activity_date, server_id
