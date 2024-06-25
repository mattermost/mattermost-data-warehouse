{{
config({
    "materialized": 'incremental',
    "incremental_strategy": "merge",
    "unique_key": ['request_id'],
    "cluster_by": ['log_date'],
    "snowflake_warehouse": "transform_l"
  })
}}

select
    request_id
    , {{ dbt_utils.generate_surrogate_key(['log_date', 'client_ip']) }} as daily_ip_id
    , log_date
    , log_at
    , client_ip
    , response_bytes
    , http_method
    , x_host_header as host_header
    , le.uri
    , status
    , referrer_url
    , user_agent
    , ua_browser_family
    , ua_os_family
    , query_string
    , cookie
    , protocol
    , request_bytes
    , time_taken
    , download_type
    , operating_system
    , version
    , version_major
    , version_minor
    , version_patch

from
    {{ ref('stg_releases__log_entries') }} le
    left join {{ ref('int_download_stats_per_uri') }} ds on le.uri = ds.uri
where
    -- Keep only requests with responses at least 1 mb
    response_bytes > 1000000
    -- Keep only completed requests
    and status like '2%'
    -- Keep only requests to download specific version
    and version is not null
    -- Keep only downloads one standard deviation to the avg
    and abs(response_bytes - ds.avg_response_bytes) < ds.stddev_response_bytes
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    and log_at >= (select max(log_at) from {{ this }})
    {% endif %}