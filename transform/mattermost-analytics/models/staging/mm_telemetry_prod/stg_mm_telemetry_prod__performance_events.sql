{{
config({
 "tags":"hourly",
})
}}

WITH MM_TELEMETRY_PROD AS 
  {{ dbt_utils.union_relations(
    relations=[
      source('mm_telemetry_prod', 'event'),
]
, include=["context_page_referrer", "channel", "context_app_namespace", "user_actual_id", 
    "context_library_name", "type", "context_app_version" , "user_actual_role", "context_page_url"
, "context_os_name" , "context_page_title"  ,  "context_app_build", "context_page_search" 
, "context_library_version",  "context_useragent", "context_app_name", "context_locale"
, "context_screen_density" , "context_page_path", "context_os_version"  , "category"  , "duration"  
, "num_of_request", "max_api_resource_size", "longest_api_resource_duration", "user_id", "userid" 
, "root_id", "post_id", "sort", "team_id", "version", "keyword", "count"
, "gfyid", "field", "plugin_id" , "installed_version", "group_constrained", "value", "include_deleted" 
, "role", "privacy", "scheme_id" , "warnmetricid"  , "metric" , "error"
, "num_invitations_sent", "num_invitations" , "channel_sidebar" , "app" , "method" , "remaining" 
, "screen" , "filter" , "version" , "uuid_ts", "id", "anonymous_id", "received_at", "sent_at", "original_timestamp", "timestamp", 
        "context_ip", "event", "event_text"], where = " CATEGORY = 'performance'",
  )}}

SELECT * FROM MM_TELEMETRY_PROD
