-- Assert that only one installation id exists for a given server id
select
    server_id, count(distinct installation_id) as server_count
from
    {{ ref('stg_mm_telemetry_prod__server') }}
where
    installation_id is not null
group by server_id
having count(distinct installation_id) > 1