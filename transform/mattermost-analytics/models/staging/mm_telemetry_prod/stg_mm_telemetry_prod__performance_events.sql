WITH performance_events AS (
    SELECT channel as channel
      , context_app_namespace as context_app_namespace
      , user_actual_id as user_actual_id
      , context_library_name as context_library_name
      , type as type
      , context_app_version as context_app_version
      , user_actual_role as user_actual_role
      , context_app_build as context_app_build
      , context_library_version as context_library_version
      , coalesce(context_useragent, context_user_agent) as context_user_agent
      , context_app_name as context_app_name
      , context_locale as context_locale
      , context_screen_density as context_screen_density
      , category as category 
      , duration as duration 
      , num_of_request as num_of_request 
      , max_api_resource_size as max_api_resource_size
      , longest_api_resource_duration as longest_api_resource_duration 
      , user_id as user_id
      , count as count
      , request_count as request_count
      , timestamp::date as event_date
      , received_at::date as received_at_date
    FROM
      {{ source('mm_telemetry_prod', 'event') }}
    WHERE CATEGORY = 'performance'
)
SELECT * FROM performance_events
