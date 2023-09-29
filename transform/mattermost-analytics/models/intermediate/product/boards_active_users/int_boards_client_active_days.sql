{{
    config({
        "materialized": "table",
        "cluster_by": ['activity_date', 'server_id'],
        "unique_key": ['activity_date', 'server_id', 'user_id'],
        "snowflake_warehouse": "transform_l"
    })
}}
with user_first_active_day as (
    select
        server_id,
        user_id,
        min(activity_date) as first_active_day
    from
        {{ ref('int_boards_client_telemetry_daily') }}
    where
        activity_date >= '{{ var('telemetry_start_date')}}'
    group by 1, 2
), spined as (
    -- Use date spine to fill in missing days
    select
        first_day.server_id,
        first_day.user_id,
        all_days.date_day
    from
        user_first_active_day first_day
        left join {{ ref('telemetry_days') }} all_days on all_days.date_day >= first_day.first_active_day
), user_daily_activity as (
    select
        cast(spined.date_day as date) as activity_date
        , spined.server_id
        , spined.user_id
        , coalesce(user_active_days.is_active, false) as is_active_today
        , max(is_active_today) over(
            partition by spined.user_id order by spined.date_day
            rows between 6 preceding and current row
        ) as is_active_last_7_days
        , max(is_active_today) over(
            partition by spined.user_id order by spined.date_day
            rows between 29 preceding and current row
        ) as is_active_last_30_days
    from
        spined
        left join {{ ref('int_boards_client_telemetry_daily') }} user_active_days
            on spined.date_day = user_active_days.activity_date
                and spined.server_id = user_active_days.server_id
                and spined.user_id = user_active_days.user_id
)
select
    {{ dbt_utils.generate_surrogate_key(['server_id', 'activity_date']) }} AS daily_server_id
    , activity_date
    , server_id
    , sum(is_active_today::integer) as daily_active_users
    , sum(is_active_last_7_days::integer) as weekly_active_users
    , sum(is_active_last_30_days::integer) as monthly_active_users
from user_daily_activity
group by activity_date, server_id