select
    {{ dbt_utils.generate_surrogate_key(['log_date', 'client_ip']) }} as daily_ip_id
    , log_date
    , client_ip
    , min(log_at) as first_download_timestamp
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
