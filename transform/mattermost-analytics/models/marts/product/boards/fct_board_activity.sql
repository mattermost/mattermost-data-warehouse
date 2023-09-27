with user_activity_daily as (
    select
        {{ dbt_utils.generate_surrogate_key(['server_id', 'activity_date']) }} AS daily_server_id
        , activity_date
        , server_id
        , sum(is_active_today::integer) as daily_active_users
        , sum(is_active_last_7_days::integer) as weekly_active_users
        , sum(is_active_last_30_days::integer) as monthly_active_users
    from {{ ref('int_boards_active_days_spined') }}
    group by activity_date, server_id
)
select
    -- Client telemetry
    t.*,
    -- Server-reported activity
    coalesce(bad.daily_active_users, 0) as server_daily_active_users,
    coalesce(bad.weekly_active_users, 0) as server_weekly_active_users,
    coalesce(bad.monthly_active_users, 0) as server_monthly_active_users,
    coalesce(bad.count_registered_users, 0) as count_registered_users,
    -- Metadata
    bad.server_id is null as is_missing_server_activity_data
from
    user_activity_daily t
    left join {{ ref('int_board_activity_latest_daily') }} bad on t.daily_server_id = bad.daily_server_id