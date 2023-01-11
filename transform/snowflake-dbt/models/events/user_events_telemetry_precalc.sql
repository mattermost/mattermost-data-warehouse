{{config({
    "materialized": "incremental",
    "schema": "events",
    "tags":"nightly",
    "snowflake_warehouse": "transform_l",
    "cluster_by": ['event_date']
  })
}}

-- User events telemetry table, but with precalculated fields. This is an attempt to temporarily improve the
-- performance of user_events_telemetry table. A better approach should consider the whole user event telemetry flows.

WITH
{% if is_incremental() %}
max_time AS (
    SELECT
        _dbt_source_relation2
        , MAX(received_at) as max_time
    FROM
        {{ this }}
    GROUP BY 1
),
{% endif %}
user_events_telemetry_precalc AS (
    SELECT
        uet.option AS option
        , uet.campaign_utm_source AS campaign_utm_source
        , uet.campaign_utm_medium AS campaign_utm_medium
        , uet.campaign_utm_campaign AS campaign_utm_campaign
        , uet.caller_info AS caller_info
        , CASE
            WHEN COALESCE(uet.type, uet.event) = 'pageview_getting_started' THEN COALESCE(uet.type, uet.event) || '_cloud'
            ELSE COALESCE(uet.type, uet.event)
          END AS type
        , CASE WHEN type = 'api_reaction_save' THEN TRUE ELSE FALSE END AS reaction_event
        , CASE WHEN type = 'LHS_DM_GM_Count' THEN TRUE ELSE FALSE END AS lhs_event
        , uet.gaexp AS gaexp
        , CASE WHEN uet.category = 'admin_team_config_page' THEN TRUE ELSE FALSE END AS admin_team_configuration_event
        , CASE WHEN uet.category = 'admin_channel_config_page' THEN TRUE ELSE FALSE END AS admin_channel_configuration_event
        -- The original query didn't define the `= 'cloud'` part. Without this part, the query is failing at snowflake
        , CASE WHEN split_part(uet.category, '_', 1) = 'cloud' THEN TRUE ELSE FALSE END AS cloud_event
        -- Following rows are results from joining other tables. If they are impacting performance, we can add them as part
        -- of the precalculation process.
        -- , CASE WHEN (split_part(${category}, '_', 1) = 'cloud') or (${server_fact.cloud_server} = TRUE) THEN TRUE ELSE FALSE END AS cloud_server
        -- , COALESCE(${customers.email}, ${portal_customers.email}) AS stripe_customer_email
        -- , COALESCE(${subscriptions.cws_dns}, ${portal_subscriptions.cws_dns}) AS stripe_customer_dns
        -- Do we really need this?
        -- , object_construct(user_events_telemetry.*) AS properties
        , uet._dbt_source_relation2 AS _dbt_source_relation2
           , CASE
                WHEN
                   SPLIT_PART(uet._dbt_source_relation2, '.', 3) IN ('SEGMENT_WEBAPP_EVENTS','RUDDER_WEBAPP_EVENTS')
                   AND LOWER(COALESCE(uet.context_user_agent, uet.context_useragent)) LIKE '%electron%'
                THEN 'Desktop'
                WHEN
                    SPLIT_PART(uet._dbt_source_relation2, '.', 3) IN ('SEGMENT_WEBAPP_EVENTS','RUDDER_WEBAPP_EVENTS')
                    AND LOWER(COALESCE(uet.context_user_agent, uet.context_useragent)) NOT LIKE '%electron%'
                THEN 'WebApp'
                WHEN SPLIT_PART(uet._dbt_source_relation2, '.', 3) IN ('SEGMENT_MOBILE_EVENTS','MOBILE_EVENTS') THEN 'Mobile'
                WHEN SPLIT_PART(uet._dbt_source_relation2, '.', 3) IN ('PORTAL_EVENTS') THEN 'Customer Portal'
                ELSE 'WebApp'
             END AS event_source
        , uet._dbt_source_relation AS _dbt_source_relation
        , uet.context_library_version AS context_library_version
        -- , COALESCE(NULLIF(${user_agent_registry.device_model}, 'Other'),uet.context_device_model) AS context_device_model
        , COALESCE(uet.context_network_cellular, FALSE) AS context_network_cellular
        , uet.context_traits_device_dimensions_width AS context_traits_device_dimensions_width
        -- , COALESCE(NULLIF(${user_agent_registry.operating_system}, 'Other'), uet.context_os_name, ${context_device_os}, ${context_traits_device_os}) AS context_os_name
        -- , CASE WHEN REGEXP_SUBSTR(${context_os_name}, '^[A-Za-z]') IS NOT NULL THEN TRUE ELSE FALSE END AS valid_operating_system
        -- , COALESCE(NULLIF(${user_agent_registry.device_type}, 'Other'),uet.context_device_type) AS context_device_type
        , uet.context_traits_anonymousid AS context_traits_anonymousid
        , uet.category AS category
        , uet.context_screen_density AS context_screen_density
        , COALESCE(uet.context_traits_ip, uet.context_ip) AS context_traits_ip
        , COALESCE(uet.context_app_name, uet.context_app_namespace) AS context_app_name
        , uet.context_useragent AS context_useragent
        , uet.context_screen_height AS context_screen_height
        , uet.event AS event
        , uet.user_actual_role AS user_actual_role
        , COALESCE(uet.anonymous_id, uet.context_traits_anonymousid) AS anonymous_id
        , uet.feature_flags AS feature_flags
        , TRIM(SPLIT_PART(SPLIT_PART(uet.feature_flags,',',1),':',2)) AS password_requirements_cloud_signup
        , TRIM(SPLIT_PART(SPLIT_PART(uet.feature_flags,',',1),':',2)) AS show_email_on_sso_password_page
        , TRIM(SPLIT_PART(SPLIT_PART(uet.feature_flags,',',2),':',2)) AS preparing_workspace_new_wording
        , TRIM(SPLIT_PART(SPLIT_PART(uet.feature_flags,',',2),':',2)) AS sso
        , uet.context_timezone AS context_timezone
        , uet.context_device_id AS context_device_id
        , uet.context_library_name AS context_library_name
        , uet.context_screen_width AS context_screen_width
        , uet.context_network_wifi AS context_network_wifi
        , COALESCE(uet.user_actual_id, anonymous_id) AS user_actual_id
        , uet.context_device_name AS context_device_name
        , uet.user_id AS user_id
        -- , COALESCE(NULLIF(${user_agent_registry.os_version}, 'Other'), uet.context_os_version) AS context_os_version
        -- , CASE WHEN nullif(SPLIT_PART(${context_os_version}, '.', 2), '') IS NULL THEN ${context_os_version} ELSE SPLIT_PART(${context_os_version}, '.',1) || '.' || SPLIT_PART(${context_os_version}, '.',2) END AS os_version_major
        , coalesce(uet.context_app_build, uet.context_traits_app_build) AS context_app_build
        , uet.name AS name
        -- Fixed typo - renamed to sign_up_sequence from sign_up_seqence
        , CASE
            WHEN COALESCE(uet.type, uet.event) IN ('cloud_signup_b_page_visit','cloud_signup_page_visit') THEN 1
            WHEN COALESCE(uet.type, uet.event) = 'click_start_trial' THEN 2
            WHEN COALESCE(uet.type, uet.event) = 'pageview_verify_email' THEN 3
            WHEN COALESCE(uet.type, uet.event) = 'enter_valid_code' THEN 4
            WHEN COALESCE(uet.type, uet.event) = 'pageview_create_workspace' AND uet.name = 'pageview_company_name' THEN 5
            WHEN COALESCE(uet.type, uet.event) = 'workspace_name_valid' THEN 6
            WHEN COALESCE(uet.type, uet.event) = 'workspace_provisioning_started' THEN 7
            WHEN COALESCE(uet.type, uet.event) = 'workspace_provisioning_ended' THEN 8
            ELSE NULL
          END AS sign_up_sequence
        , CASE
            WHEN COALESCE(uet.type, uet.event) IN ('cloud_signup_b_page_visit','cloud_signup_page_visit') THEN 'Sign-Up Page'
            WHEN COALESCE(uet.type, uet.event) = 'click_start_trial' THEN 'Click Start Trial'
            WHEN COALESCE(uet.type, uet.event) = 'pageview_verify_email' THEN 'Verify Email'
            WHEN COALESCE(uet.type, uet.event) = 'enter_valid_code' THEN 'Enter Valid Code'
            WHEN COALESCE(uet.type, uet.event) = 'pageview_create_workspace' AND uet.name = 'pageview_company_name' THEN 'Enter Company Name'
            WHEN COALESCE(uet.type, uet.event) = 'workspace_name_valid' THEN 'Workspace Name Valid'
            WHEN COALESCE(uet.type, uet.event) = 'workspace_provisioning_started' THEN 'Workspace Creation Started'
            WHEN COALESCE(uet.type, uet.event) = 'workspace_provisioning_ended' THEN 'Workspace Creation Ended'
            ELSE NULL
          END AS sign_up_events_sequence
        , uet.context_traits_server AS context_traits_server
        , COALESCE(uet.context_traits_device_istablet, uet.context_device_is_tablet, false) AS context_traits_device_istablet
        , uet.context_traits_app_build AS context_traits_app_build
        , uet.context_traits_device_os AS context_traits_device_os
        , uet.context_app_namespace AS context_app_namespace
        -- , COALESCE(NULLIF(${user_agent_registry.device_brand}, 'Other'),uet.context_device_manufacturer) AS context_device_manufacturer
        , uet.context_network_bluetooth AS context_network_bluetooth
        , uet.context_locale AS context_locale
        , uet.context_traits_id AS context_traits_id
        , uet.context_traits_userid AS context_traits_userid
        , uet.context_network_carrier AS context_network_carrier
        , uet.id AS id
        , uet.event_text AS event_text
        , COALESCE(uet.context_app_version, uet.context_traits_app_version) AS context_app_version
        , uet.context_ip AS context_ip
        , uet.channel AS channel
        , uet.context_traits_app_version AS context_traits_app_version
        , uet.context_traits_device_dimensions_height AS context_traits_device_dimensions_height
        , uet.from_background AS from_background
        , uet.channel_id AS channel_id
        , uet.context_page_url AS context_page_url
        , uet.context_page_referrer AS context_page_referrer
        , uet.page AS page
        , uet.context_page_title AS context_page_title
        , uet.context_page_search AS context_page_search
        , uet.context_page_path AS context_page_path
        , uet.subscription_id AS subscription_id
        , uet.context_traits_portal_customer_id AS context_traits_portal_customer_id
        , uet.portal_customer_id AS portal_customer_id
        , uet.stripe_error AS stripe_error
        , uet.workspace_name AS workspace_name
        , uet.suggestion AS suggestion
        , uet.duration AS duration
        , uet.root_id AS root_id
        , uet.post_id AS post_id
        , uet.sort AS sort
        , uet.team_id AS team_id
        , uet.userid AS userid
        , uet.version AS version
        , uet.keyword AS keyword
        , uet.count AS count
        , uet.gfyid AS gfyid
        , uet.context AS context
        , uet.field AS field
        , uet.plugin_id AS plugin_id
        , uet.installed_version AS installed_version
        , uet.group_constrained AS group_constrained
        , uet.value AS value
        , uet.include_deleted AS include_deleted
        , uet.role AS role
        , CASE
            WHEN uet.privacy = 'O' THEN 'Open'
            WHEN uet.privacy = 'P' THEN 'Private'
            ELSE uet.privacy
          END AS privacy
        , uet.scheme_id AS scheme_id
        , uet.channelsids AS channelsids
        , uet.channel_ids AS channel_ids
        , uet.from_page AS from_page
        , uet.context_compiled AS context_compiled
        , uet.context_terminators_lastindex AS context_terminators_lastindex
        , uet.context_contains AS context_contains
        , uet.context_relevance AS context_relevance
        , uet.warnmetricid AS warnmetricid
        , uet.metric AS metric
        , uet.context_traits_cross_domain_id AS context_traits_cross_domain_id
        , uet.context_amp_id AS context_amp_id
        , uet.channel_name AS channel_name
        , uet.context_campaign_medium AS context_campaign_medium
        , uet.context_campaign_source AS context_campaign_source
        , uet.team_name AS team_name
        , uet.channel_id_tid AS channel_id_tid
        , uet.context_campaign_name AS context_campaign_name
        , uet.channel_id_value AS channel_id_value
        , uet.context_campaign_content AS context_campaign_content
        , uet.context_campaign_term AS context_campaign_term
        , uet.segment_dedupe_id AS segment_dedupe_id
        , COALESCE(uet.context_user_agent, uet.context_useragent) AS context_user_agent
        , uet.context_server AS context_server
        , uet.context_device_os AS context_device_os
        , uet.context_device_is_tablet AS context_device_is_tablet
        , uet.context_device_dimensions_height AS context_device_dimensions_height
        , uet.context_device_dimensions_width AS context_device_dimensions_width
        , uet.timestamp AS timestamp
        , uet.password_requirements AS password_requirements
        , uet.oauth_provider AS oauth_provider
        , uet.context_traits_use_oauth AS context_traits_use_oauth
        , uet.first AS first
        , uet.second AS second
        , uet.third AS third
        , uet.first_effectiveness AS first_effectiveness
        , uet.first_recomputations AS first_recomputations
        , uet.second_effectiveness AS second_effectiveness
        , uet.second_recomputations AS second_recomputations
        , uet.third_effectiveness AS third_effectiveness
        , uet.third_recomputations AS third_recomputations
        , DAYNAME(uet.timestamp) AS event_dayname
        , EXTRACT(dayofweek FROM uet.timestamp)::int AS event_day
        , WEEKISO(uet.timestamp)::int AS event_week_of_year
        -- Extra columns compared to original user_events_telemetry
        -- Keep only date from event. Useful for date range operations. Can be used as the clustering key.
        , uet.timestamp::date AS event_date
        -- To be used for incremental update
        , uet.received_at AS received_at
    FROM
        {{ ref('user_events_telemetry') }} uet
        {% if is_incremental() %}
        JOIN mt ON uet._dbt_source_relation2 = mt._dbt_source_relation2
        {% endif %}
    WHERE
        -- Don't add records from the future - at least not yet
        uet.received_at <= CURRENT_TIMESTAMP
        {% if is_incremental() %}
        AND uet.received_at > mt.max_time
        {% endif %}
)
SELECT
    *
FROM
    user_events_telemetry_precalc

