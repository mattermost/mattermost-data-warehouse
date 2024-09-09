-- Assert that only one server id exists for a given installation id
select
    installation_id, count(distinct server_id) as server_count
from
    {{ ref('stg_mm_telemetry_prod__server') }}
where
    installation_id is not null
group by installation_id
having count(distinct server_id) > 1