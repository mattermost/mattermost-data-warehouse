select
    server_id,
    array_agg(reason) as reasons
from
    {{ ref('int_server_info_daily_snapshot') }}
group by server_id