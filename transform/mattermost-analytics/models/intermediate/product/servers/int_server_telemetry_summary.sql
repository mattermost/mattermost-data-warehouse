
select
    server_id,
    min(activity_date) over (partition by server_id) as first_activity_date,
    max(activity_date) over (partition by server_id ) as last_activity_date,
    first_value(binary_edition) over (partition by server_id order by activity_date asc) as first_binary_edition,
    last_value(binary_edition) over (partition by server_id order by activity_date asc) as last_binary_edition,
    first_value(count_registered_active_users) over (partition by server_id order by activity_date asc) as first_count_registered_active_users,
    last_value(count_registered_active_users) over (partition by server_id order by activity_date asc) as last_count_registered_active_users,
    last_value(daily_active_users) over (partition by server_id order by activity_date asc) as last_daily_active_users,
    last_value(monthly_active_users) over (partition by server_id order by activity_date asc) as last_monthly_active_users,
    last_value(server_ip) over (partition by server_id order by activity_date asc) as last_server_ip
from
    {{ ref('int_server_active_days_spined') }}
-- Keep only one row per server as the current query creates duplicates
qualify row_number() over (partition by server_id order by server_id) = 1