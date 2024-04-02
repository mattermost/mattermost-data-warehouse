
with server_date_range as (
    select
        server_id
        , min(activity_date) as first_active_day
        , max(activity_date) as last_active_day
    from
        {{ ref('int_feature_daily_spine') }}
    where
        activity_date >= '{{ var('telemetry_start_date')}}'
    group by
        server_id
)
select
    sdr.server_id
    , sdr.first_active_day
    , sdr.last_active_day
    ,
from
    server_date_range sdr
    left join {{ ref('int_server_hosting_type') }} ht on si.server_id = ht.server_id
