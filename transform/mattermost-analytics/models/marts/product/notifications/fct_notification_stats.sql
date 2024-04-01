select
    spine.date_hour,
    coalesce(eu_logs.count_send_push + logs_eu.count_send_push, 0) as count_eu_send_push,
    coalesce(eu_logs.count_send_ack + logs_eu.count_send_ack, 0) as count_eu_send_ack,
    coalesce(us_logs.count_send_push + logs_us.count_send_push, 0) as count_us_send_push,
    coalesce(us_logs.count_send_ack + logs_us.count_send_ack, 0) as count_us_send_ack,
    coalesce(test_logs.count_send_push + logs_test.count_send_push, 0) as count_test_send_push,
    coalesce(test_logs.count_send_ack + logs_test.count_send_ack, 0) as count_test_send_ack
from
    {{ ref('notification_date_hour') }} spine
    left join {{ ref('int_notifications_eu_hourly') }} eu_logs on spine.date_hour = eu_logs.request_date_hour
    left join {{ ref('int_notifications_us_hourly') }} us_logs on spine.date_hour = us_logs.request_date_hour
    left join {{ ref('int_notifications_test_hourly') }} test_logs on spine.date_hour = test_logs.request_date_hour
    left join {{ ref('int_notifications_logs_eu_hourly') }} logs_eu on spine.date_hour = logs_eu.request_date_hour
    left join {{ ref('int_notifications_logs_us_hourly') }} logs_us on spine.date_hour = logs_us.request_date_hour
    left join {{ ref('int_notifications_logs_test_hourly') }} logs_test on spine.date_hour = logs_test.request_date_hour

