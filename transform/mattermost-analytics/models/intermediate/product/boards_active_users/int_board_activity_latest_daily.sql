with id_mapping as (
    select
        distinct server_id, telemetry_id
    from
        {{ ref('stg_hacktoberboard_prod__server') }}
)
select
    m.server_id,
    CAST(a.timestamp AS date) AS server_date,
    {{ dbt_utils.generate_surrogate_key(['m.server_id', 'server_date']) }} AS daily_server_id,
    a.daily_active_users,
    a.weekly_active_users,
    a.monthly_active_users,
    a.count_registered_users
from
    {{ ref('stg_hacktoberboard_prod__activity') }} a
    left join id_mapping m on m.telemetry_id = a.telemetry_id
where
    -- Ignore rows where server date is in the future.
    server_date <= CURRENT_DATE()

-- Keep latest record per day
qualify row_number() over (partition by m.server_id, server_date order by timestamp desc) = 1