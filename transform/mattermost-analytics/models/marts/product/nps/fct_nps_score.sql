with nps_server_daily_score as (
    select {{ dbt_utils.generate_surrogate_key(['event_date', 'server_id']) }} as id
      , event_date as event_date
      , server_id as server_id
      , server_version as server_version
      , user_role as user_role
      , min(server_install_date) as server_install_date
      , count(distinct case when score > 8 then user_id else null end) as promoters
      , count(distinct case when score <= 6 then user_id else null end) as detractors
      , count(distinct case when score > 6 and score <= 8 then user_id else null end) as passives
      , count(distinct user_id) as nps_users
      , avg(score) as avg_score
      , 100.0 * ((promoters / nps_users) - (detractors / nps_users)) as nps_score
    from {{ ref('int_nps_feedback') }}
    group by event_date
      , server_id 
      , server_version 
      , user_role
)
select *
from nps_server_daily_score