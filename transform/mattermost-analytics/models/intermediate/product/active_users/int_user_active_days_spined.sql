{{
    config({
        "materialized": "table",
        "cluster_by": ['activity_date', 'server_id'],
        "unique_key": ['activity_date', 'server_id', 'user_id'],
        "snowflake_warehouse": "transform_l"
    })
}}

{% set metrics = ['is_active', 'is_desktop_or_server', 'is_mobile', 'is_old_server'] %}

with user_active_days as (
    -- Merge mobile with server data
    select
        -- Load only required columns
        coalesce(s.activity_date, m.activity_date, l.activity_date) as activity_date,
        coalesce(s.server_id, m.server_id, l.server_id) as server_id,
        coalesce(s.user_id, m.user_id, l.user_id) as user_id,
        coalesce(s.is_active, m.is_active, l.is_active) as is_active,
        s.server_id is not null as is_desktop_or_server,
        m.server_id is not null as is_mobile,
        l.server_id is not null as is_old_server
    from
        {{ ref('int_user_active_days_server_telemetry') }} s
        full outer join {{ ref('int_user_active_days_mobile_telemetry') }} m on s.daily_user_id = m.daily_user_id
        full outer join {{ ref('int_user_active_days_legacy_telemetry') }} l on s.daily_user_id = l.daily_user_id
), user_first_active_day as (
    select
        server_id,
        user_id,
        min(activity_date) as first_active_day
    from
        user_active_days
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
)
select
    cast(spined.date_day as date) as activity_date
    , spined.server_id
    , spined.user_id

{% for metric in metrics %}
    , coalesce(user_active_days.{{metric}}, false) as {{metric}}_today
    , max({{metric}}_today) over(
        partition by spined.user_id order by spined.date_day
        rows between 6 preceding and current row
    ) as {{metric}}_last_7_days
    , max({{metric}}_today) over(
        partition by spined.user_id order by spined.date_day
        rows between 29 preceding and current row
    ) as {{metric}}_last_30_days
{% endfor %}
from
    spined
    left join user_active_days
        on spined.date_day = user_active_days.activity_date
            and spined.server_id = user_active_days.server_id
            and spined.user_id = user_active_days.user_id