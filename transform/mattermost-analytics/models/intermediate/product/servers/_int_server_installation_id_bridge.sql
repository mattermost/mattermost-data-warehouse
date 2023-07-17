select
    server_id,
    installation_id
from
    -- Installation ID exists only in rudderstack telemetry
    {{ ref('int_server_telemetry_latest_daily') }}
where
    installation_id is not null
group by 1, 2