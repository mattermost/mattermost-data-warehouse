{{
config({
    "materialized": 'incremental',
    "incremental_strategy": "merge",
    "unique_key": ['request_date_hour'],
    "cluster_by": ['request_date_hour'],
    "snowflake_warehouse": "transform_l"
  })
}}


select
    date_trunc(hour, request_at) as request_date_hour,
    count_if(url like '%/api/v1/send_push') as count_send_push,
    count_if(url like '%/api/v1/ack') as count_send_ack
from
    {{ ref('stg_push_proxy__logs_us_new') }}
where
    http_method = 'POST'
    and url like any ('%/api/v1/send_push', '%/api/v1/ack')
    -- Need only successful requests
    and elb_status_code = 200
    -- Only last two years worth of data are needed
    and request_at > '{{ var('notification_start_date') }}'
    {% if is_incremental() %}
       -- this filter will only be applied on an incremental run
      and request_at >= (select max(request_date_hour) from {{ this }})
    {% endif %}
group by 1