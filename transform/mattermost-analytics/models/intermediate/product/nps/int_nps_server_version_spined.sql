with first_server_version as 
(
    select 
        server_id
        , min(event_date) min_event_date
    from {{ ref('int_nps_score') }}
    where event_date >= '{{ var('telemetry_start_date')}}'
        group by all
)
, spined as (
    select 
        date_day::date as activity_date
        , server_id as server_id
    from first_server_version fsd
    left join {{ ref('telemetry_days') }} td 
        on date_day::date >= min_event_date
) 
, server_version_cte as (
    select 
        sp.activity_date
        , sp.server_id
        , FIRST_VALUE(nps_score.server_version IGNORE NULLS) OVER (
            PARTITION BY sp.server_id 
            ORDER BY activity_date DESC
            ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
            AS server_version
    from spined sp 
    left join {{ ref('int_nps_score') }} nps_score 
    on sp.server_id = nps_score.server_id  and sp.activity_date = nps_score.event_date
) select * from server_version_cte 
    order by activity_date desc


