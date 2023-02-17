{{
    config({
        "tags":"hourly",
    })
}}

WITH MM_TELEMETRY_PROD AS 
(SELECT _dbt_source_relation	  AS _dbt_source_relation	
 , context_page_referrer          AS context_page_referrer
 , channel                        AS channel
 , context_app_namespace          AS context_app_namespace
 , id                             AS id
 , user_actual_id                 AS user_actual_id
 , context_library_name           AS context_library_name
 , type                           AS type
 , context_app_version            AS context_app_version
 , user_actual_role               AS user_actual_role
 , context_page_url               AS context_page_url
 , context_os_name                AS context_os_name
 , context_page_title             AS context_page_title
 , anonymous_id                   AS anonymous_id
 , context_app_build              AS context_app_build
 , context_page_search            AS context_page_search
 , context_library_version        AS context_library_version
 , context_ip                     AS context_ip
 , context_useragent              AS context_useragent
 , context_app_name               AS context_app_name
 , context_locale                 AS context_locale
 , context_screen_density         AS context_screen_density
 , context_page_path              AS context_page_path
 , context_os_version             AS context_os_version
 , category                       AS category
 , duration                       AS duration
 , num_of_request                 AS num_of_request
 , max_api_resource_size          AS max_api_resource_size
 , longest_api_resource_duration  AS longest_api_resource_duration
 , user_id                        AS user_id
 , userid                         AS userid
 , root_id                        AS root_id
 , post_id                        AS post_id
 , sort                           AS sort
 , team_id                        AS team_id
 , version                        AS version
 , keyword                        AS keyword
 , count                          AS count
 , gfyid                          AS gfyid
 , field                          AS field
 , plugin_id                      AS plugin_id
 , installed_version              AS installed_version
 , group_constrained              AS group_constrained
 , value                          AS value
 , include_deleted                AS include_deleted
 , role                           AS role
 , privacy                        AS privacy
 , scheme_id                      AS scheme_id
 , warnmetricid                   AS warnmetricid
 , metric                         AS metric
 , error                          AS error
 , num_invitations_sent           AS num_invitations_sent
 , num_invitations                AS num_invitations
 , channel_sidebar                AS channel_sidebar
 , app                            AS app
 , method                         AS method
 , remaining                      AS remaining
 , screen                         AS screen
 , filter                         AS filter
 , version                        AS version
 , original_timestamp             AS original_timestamp
 , sent_at                        AS sent_at
 , uuid_ts                        AS uuid_ts
 , received_at                    AS received_at 
 FROM {{ ref ('events') }} WHERE category = 'performance')
 SELECT * FROM MM_TELEMETRY_PROD