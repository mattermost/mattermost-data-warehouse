with base_cte as (
     select event_date
          , server_id
          , server_version
          , user_role
          , count(distinct case when nps_score.score > 8 then nps_score.user_id else null end) as promoters
          , count(distinct case when nps_score.score < 7 then nps_score.user_id else null end) as detractors
          , count(distinct case when nps_score.score > 6 and nps_score.score < 9 then nps_score.user_id else null end) as passives
          , count(distinct user_id) as nps_users 
     from {{ ref('int_nps_score') }}  nps_score
     group by event_date
          , server_id
          , server_version
          , user_role
), spined as (
     select td.date_day::date activity_date
          , server_id
          , user_role
          , case when td.date_day::date = event_date then promoters else 0 end promoters
          , case when td.date_day::date = event_date then detractors else 0 end detractors
          , case when td.date_day::date = event_date then passives else 0 end passives
          , case when td.date_day::date = event_date then nps_users else 0 end nps_users
          , max(server_version) server_version
     from {{ ref('telemetry_days') }} td 
     left join base_cte b on td.date_day::date >= event_date 
     group by td.date_day::date 
          , server_id
          , user_role
          , case when td.date_day::date = event_date then promoters else 0 end 
          , case when td.date_day::date = event_date then detractors else 0 end 
          , case when td.date_day::date = event_date then passives else 0 end 
          , case when td.date_day::date = event_date then nps_users else 0 end 
     having td.date_day::date >= min(event_date)
)
     select spined.*
          , sum(promoters) over (partition by activity_date order by activity_date desc rows between current row and 89 following) as promoters_last_90_days
          , sum(detractors) over (partition by activity_date order by activity_date desc rows between current row and 89 following) as detractors_last_90_days
          , sum(passives) over (partition by activity_date order by activity_date desc rows between current row and 89 following) as passives_last_90_days
          , sum(nps_users) over (partition by activity_date order by activity_date desc rows between current row and 89 following) as nps_users_last_90_days
     from spined
     group by activity_date, server_id, user_role, promoters, detractors, passives, nps_users, server_version
     order by activity_date desc



