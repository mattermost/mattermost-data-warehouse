select
    uri
    , avg(response_bytes) as avg_response_bytes
    , stddev(response_bytes) as stddev_response_bytes
    , max(response_bytes) as max_response_bytes
from
    {{ ref('stg_releases__log_entries') }}
where
    -- Keep only requests with responses
    response_bytes > 0
    -- Keep only completed requests
    and status like '2%'
    -- Keep only requests to download specific version
    and version is not null
group by uri
