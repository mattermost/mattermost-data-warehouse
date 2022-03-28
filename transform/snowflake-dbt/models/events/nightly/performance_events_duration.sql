{{config({
    "materialized": "incremental",
    "schema": "events",
    "tags":"nightly",
    "snowflake_warehouse": "transform_xs"
  })
}}

WITH P_E AS (
SELECT context_useragent,context_app_name,context_locale,context_screen_density,context_page_path,context_os_version,category,duration,user_id,
root_id,post_id,sort,team_id,keyword,gfyid,field,plugin_id,installed_version,group_constrained,value,include_deleted,role,
privacy,scheme_id,warnmetricid,metric,error,num_invitations_sent,num_invitations,channel_sidebar,app,method,remaining,screen,filter,version,
 original_timestamp,sent_at,uuid_ts,timestamp
 FROM {{ ref('performance_events') }}
{% if is_incremental() %}

WHERE timestamp > (SELECT MAX(timestamp) from {{this}})

{% endif %}
), PCT_5_95 AS (
SELECT percentile_cont(.95) WITHIN GROUP (ORDER BY DURATION) DURATION_P95
    , percentile_cont(.5) WITHIN GROUP (ORDER BY DURATION) DURATION_P5
    FROM P_E
)
SELECT * FROM P_E P_E 
JOIN PCT_5_95 PCT_5_95 
WHERE P_E.DURATION > DURATION_P5 AND  P_E.DURATION < DURATION_P95
