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
    eu_logs.count_send_ack as count_eu_send_ack,
    us_logs.count_send_push as count_us_send_push,
    us_logs.count_send_ack as count_us_send_ack,
    test_logs.count_send_push as count_test_send_push,
    test_logs.count_send_ack as count_test__send_ack
from
    {{ ref('notification_date_hour') }} spine
    left join {{ ref('int_notifications_eu_hourly') }} eu_logs on spine.date_hour = eu_logs.date_hour
    left join {{ ref('int_notifications_us_hourly') }} us_logs on spine.date_hour = us_logs.date_hour
    left join {{ ref('int_notifications_test_hourly') }} test_logs on spine.date_hour = test_logs.date_hour
