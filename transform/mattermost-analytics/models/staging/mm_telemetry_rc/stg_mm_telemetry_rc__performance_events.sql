{%- set include_columns = ["context_page_referrer", "channel", "context_app_namespace", "user_actual_id"
, "context_library_name", "type", "context_app_version" , "user_actual_role", "context_page_url", "context_os_name" 
, "context_page_title", "context_app_build", "context_page_search" , "context_library_version"
, "context_useragent", "context_app_name", "context_locale", "context_screen_density"  
, "context_page_path", "context_os_version", "category" , "duration" , "num_of_request", "max_api_resource_size"
, "longest_api_resource_duration" , "user_id", "userid", "root_id", "post_id", "sort" 
, "team_id", "version", "keyword", "count", "gfyid", "field"
, "plugin_id", "installed_version", "group_constrained", "value", "include_deleted" 
, "role", "privacy", "scheme_id", "metric", "error", "num_invitations_sent" 
, "num_invitations", "channel_sidebar", "app", "method", "remaining", "screen" 
, "filter", "version", "uuid_ts"] 
-%}
        
WITH performance_event AS (
    SELECT
      {{ get_rudderstack_columns() }}
        , {% for column in include_columns %}
        {{ column }} AS {{ column }}
        {% if not loop.last %},{% endif %}
    {% endfor %}
    , timestamp::date as event_date
    , received_at::date as received_at_date
    FROM
      {{ source('mm_telemetry_rc', 'event') }}
    WHERE CATEGORY = 'performance'
)
SELECT * FROM performance_event


 