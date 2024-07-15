select
    server_id,
    CAST(timestamp AS date) AS server_date,
    {{ dbt_utils.generate_surrogate_key(['server_id', 'server_date']) }} AS daily_server_id,
    daily_active_users,
    monthly_active_users,
    count_registered_users,
    count_registered_deactivated_users,
    count_public_channels,
    count_private_channels,
    count_teams,
    count_slash_commands,
    count_direct_message_channels,
    count_posts
from
    {{ ref('stg_mattermost2__activity') }}
where
    -- Ignore rows where server date is in the future.
    server_date <= CURRENT_DATE()
-- Keep latest record per day
qualify row_number() over (partition by server_id, server_date order by timestamp desc) = 1
