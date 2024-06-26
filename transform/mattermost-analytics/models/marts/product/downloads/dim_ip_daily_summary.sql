{{
config({
    "materialized": 'incremental',
    "incremental_strategy": "delete+insert",
    "unique_key": ['daily_ip_id'],
    "cluster_by": ['log_date'],
    "snowflake_warehouse": "transform_l"
  })
}}
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
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    and log_date >= (select max(log_date) from {{ this }})
    {% endif %}
group by
    log_date
    , client_ip
