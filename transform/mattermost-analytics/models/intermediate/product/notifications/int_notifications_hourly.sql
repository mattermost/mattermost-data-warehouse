-- Temporarily materialize as table
{{
config({
    "materialized": 'table',
    "snowflake_warehouse": "transform_l"
  })
}}


select
    spine.date_hour,
    eu_logs.count_send_push as count_eu_send_push,
    coalesce(eu_logs.count_send_ack, 0) as count_eu_send_ack,
    coalesce(us_logs.count_send_push, 0) as count_us_send_push,
    coalesce(us_logs.count_send_ack, 0) as count_us_send_ack,
    coalesce(test_logs.count_send_push, 0) as count_test_send_push,
    coalesce(test_logs.count_send_ack, 0) as count_test__send_ack
from
    {{ ref('notification_date_hour') }} spine
    left join {{ ref('int_notifications_eu_hourly') }} eu_logs on spine.date_hour = eu_logs.request_date_hour
    left join {{ ref('int_notifications_us_hourly') }} us_logs on spine.date_hour = us_logs.request_date_hour
    left join {{ ref('int_notifications_test_hourly') }} test_logs on spine.date_hour = test_logs.request_date_hour
