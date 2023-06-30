select
    server_id,
    array_agg(reason) as reasons
from
    {{ ref('int_excludable_servers') }}
group by server_id