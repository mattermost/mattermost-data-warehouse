select
    spine.date_hour,
    {{ dbt_utils.safe_add(['eu_logs.count_send_push', 'logs_eu.count_send_push']) }} as count_eu_send_push,
    {{ dbt_utils.safe_add(['eu_logs.count_send_ack', 'logs_eu.count_send_ack']) }} as count_eu_send_ack,
    {{ dbt_utils.safe_add(['us_logs.count_send_push', 'logs_us.count_send_push']) }} as count_us_send_push,
    {{ dbt_utils.safe_add(['us_logs.count_send_ack', 'logs_us.count_send_ack']) }} as count_us_send_ack,
    {{ dbt_utils.safe_add(['test_logs.count_send_push', 'logs_test.count_send_push']) }} as count_test_send_push,
    {{ dbt_utils.safe_add(['test_logs.count_send_ack', 'logs_test.count_send_ack']) }} as count_test_send_ack
from
    {{ ref('notification_date_hour') }} spine
    left join {{ ref('int_notifications_eu_hourly') }} eu_logs on spine.date_hour = eu_logs.request_date_hour
    left join {{ ref('int_notifications_us_hourly') }} us_logs on spine.date_hour = us_logs.request_date_hour
    left join {{ ref('int_notifications_test_hourly') }} test_logs on spine.date_hour = test_logs.request_date_hour
    left join {{ ref('int_notifications_logs_eu_hourly') }} logs_eu on spine.date_hour = logs_eu.request_date_hour
    left join {{ ref('int_notifications_logs_us_hourly') }} logs_us on spine.date_hour = logs_us.request_date_hour
    left join {{ ref('int_notifications_logs_test_hourly') }} logs_test on spine.date_hour = logs_test.request_date_hour

