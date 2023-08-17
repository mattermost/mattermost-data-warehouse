select
    server_id,
    array_agg(reason) within group (order by reason) as reasons
from
    {{ ref('int_excludable_servers') }}
where
    server_id is not null
group by server_id