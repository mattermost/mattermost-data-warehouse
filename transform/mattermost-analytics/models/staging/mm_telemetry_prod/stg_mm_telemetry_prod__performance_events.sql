{%- set include_columns = ["context_page_referrer", "channel", "context_app_namespace", "user_actual_id"
, "context_library_name", "type", "context_app_version" , "user_actual_role", "context_page_url", "context_os_name" 
, "context_page_title", "context_app_build", "context_page_search" , "context_library_version"
, "context_useragent", "context_app_name", "context_locale", "context_screen_density" 
, "context_page_path", "context_os_version", "category" , "duration" , "num_of_request", "max_api_resource_size"
, "longest_api_resource_duration" , "user_id", "count", "uuid_ts"] -%}
        
WITH performance_events AS (
    SELECT
      {{ get_rudderstack_columns() }}
        , {% for column in include_columns %}
        {{ column }} AS {{ column }}
        {% if not loop.last %},{% endif %}
    {% endfor %}
    , timestamp::date as event_date
    , received_at::date as received_at_date
    FROM
      {{ source('mm_telemetry_prod', 'event') }}
    WHERE CATEGORY = 'performance'
)
SELECT * FROM performance_events
