select
    installation_id,
    min(activity_date) over (partition by installation_id) as first_activity_date,
    max(activity_date) over (partition by installation_id) as last_activity_date,
    first_value(binary_edition) over (partition by installation_id order by activity_date asc) as first_binary_edition,
    last_value(binary_edition) over (partition by installation_id order by activity_date asc) as last_binary_edition,
    first_value(count_registered_active_users) over (partition by installation_id order by activity_date asc) as first_count_registered_active_users,
    last_value(count_registered_active_users) over (partition by installation_id order by activity_date asc) as last_count_registered_active_users,
    last_value(daily_active_users) over (partition by installation_id order by activity_date asc) as last_daily_active_users,
    last_value(monthly_active_users) over (partition by installation_id order by activity_date asc) as last_monthly_active_users,
    last_value(server_id) over (partition by installation_id order by activity_date asc) as last_server_id,
    count(distinct server_id) over (partition by installation_id) as count_server_ids
from
    {{ ref('int_server_active_days_spined') }}
where
    installation_id is not null
qualify row_number() over(partition by installation_id order by activity_date desc) = 1
