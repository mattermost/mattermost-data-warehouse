select
    server_id,
    CAST(timestamp AS date) AS server_date,
    {{ dbt_utils.generate_surrogate_key(['server_id', 'server_date']) }} AS daily_server_id,
    daily_active_users,
    monthly_active_user,
    count_registered_users,
    registered_deactivated_users
from
    {{ ref('stg_mm_telemetry_prod__activity') }}
where
    -- Ignore rows where server date is in the future.
    server_date <= CURRENT_DATE()
-- Keep latest record per day
qualify row_number() over (partition by server_id, server_date order by timestamp desc) = 1