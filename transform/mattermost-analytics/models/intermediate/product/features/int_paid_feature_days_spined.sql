{%- set mau_days = 29 -%}

with server_paid_feature_date_range as (
    select
        server_id,
        min(server_date) as first_server_date,
        max(server_date) as last_server_date
    from
        {{ ref('int_paid_feature_daily_usage_per_user') }}
    where
        server_date >= '{{ var('telemetry_start_date')}}'
    group by
        server_id
), spine as (
    select
        sd.server_id,
        all_days.date_day::date as activity_date,
        {{ dbt_utils.generate_surrogate_key(['server_id', 'activity_date']) }} AS daily_server_id
    from
        server_paid_feature_date_range sd
        left join {{ ref('telemetry_days') }} all_days
            on all_days.date_day >= sd.first_active_day and all_days.date_day <= dateadd(day, sd.last_active_day, {{mau_days}})
)
-- Temporary commit
select * from spine