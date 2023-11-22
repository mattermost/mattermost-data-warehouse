  with retention_at_28 as (
    select
        fct.server_id,
        fct.daily_active_users,
        fct.server_daily_active_users
    from
        {{ ref('fct_active_users') }} fct
        join {{ ref('dim_daily_server_info') }} dsi on fct.daily_server_id = dsi.daily_server_id and dsi.age_in_days = 28
  )
  select
      si.server_id,
      si.first_activity_date as active_since_date,
      r.daily_active_users is not null as is_active_at_day_28,
      coalesce(r.daily_active_users, 0) as daily_active_users,
      coalesce(r.server_daily_active_users, 0) as server_daily_active_users
  from
      {{ ref('dim_server_info') }} si
      left join retention_at_28 r on si.server_id = r.server_id