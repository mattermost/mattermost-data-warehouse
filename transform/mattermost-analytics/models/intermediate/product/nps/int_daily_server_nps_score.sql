with server_min_score_date as (
    select server_id as server_id
        , server_version as server_version
        , user_role as user_role
        , min(event_date) as min_score_date
    from {{ ref('int_nps_score') }}
    where event_date >= '{{ var('telemetry_start_date')}}'
    group by server_id 
        , server_version 
        , user_role
), 
spined as (
        select all_days.date_day
            , server_min_score_date.server_id as server_id
            , server_min_score_date.server_version as server_version
            , server_min_score_date.user_role as user_role
        from nps_server_daily_score nps 
        left join {{ ref('telemetry_days') }} all_days on all_days.date_day >= server_min_score_date.min_score_date
), server_daily_nps_score_activity as (
    select
        cast(spined.date_day as date) as activity_date
        , spined.server_id
        , spined.server_version
        , spined.user_role
        , count(distinct case when nps_score.score > 8 then nps_score.user_id else null end) as promoters
        , count(distinct case when nps_score.score < 7 then nps_score.user_id else null end) as detractors
        , count(distinct case when nps_score.score > 6 and nps_score.score < 9 then nps_score.user_id else null end) as passives
        , count(distinct nps_score.user_id) as nps_users
    from
        spined
        left join {{ ref('int_nps_score') }} nps_score
            on spined.date_day = nps_score.event_date
                and spined.server_id = nps_score.server_id
                and spined.server_version = nps_score.server_version
                and spined.user_role = nps_score.user_role
) select * 
        from server_daily_nps_score_activity
