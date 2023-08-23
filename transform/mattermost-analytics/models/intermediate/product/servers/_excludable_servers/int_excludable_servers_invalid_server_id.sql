select
    server_id,
    'Invalid server id'
from
    {{ ref('int_server_summary') }}
where
    length(server_id) <> 26