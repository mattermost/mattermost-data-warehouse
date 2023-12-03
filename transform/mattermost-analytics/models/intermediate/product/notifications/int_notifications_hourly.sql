-- Temporarily materialize as table
{{
config({
    "materialized": 'table',
  })
}}

{% set log_tables = {"stg_push_proxy__de_logs": "Europe", "stg_push_proxy__test_logs": "Test", "stg_push_proxy__us_logs": "US"} %}


with
{% for table in log_tables.keys() %}
-- Aggregate each server's logs hourly, counting the number of push notification and acknowledgement requests
{{table}}_hourly as (
    select
        date_trunc(hour, request_at) as request_date_hour,
        elb_status_code as status_code,
        count_if(url like '%/api/v1/send_push') as send_push,
        count_if(url like '%/api/v1/ack') as send_ack
    from
        {{ ref(table) }}
    where
        http_method = 'POST'
        and url like any ('%/api/v1/send_push', '%/api/v1/ack')
    group by 1, 2
)
{% if not loop.last %},{% endif %}
{% endfor %}

{% for table, server in log_tables.items() %}
select
    request_date_hour,
    status_code,
    send_push,
    send_ack,
    '{{server}}' as server
from
    {{table}}_hourly

{% if not loop.last %}union all{% endif %}

{% endfor %}
