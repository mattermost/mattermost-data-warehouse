{{config({
    "materialized": 'incremental',
    "unique_key": 'id',
    "schema": "web"
  })
}}

WITH daily_website_traffic AS (
      SELECT
        timestamp::DATE                                                                           AS date
      , anonymous_id
      , channel
      , context_app_build
      , context_app_name
      , context_app_namespace
      , context_app_version
      , context_ip
      , context_library_name
      , context_library_version
      , context_locale
      , context_os_name
      , context_os_version
      , context_page_path
      , context_page_referrer
      , context_page_search
      , context_page_title
      , context_page_url
      , context_screen_density
      , context_traits_portal_customer_id
      , context_useragent
      , name
      , objectobjectpath
      , objectobjectreferrer
      , objectobjectsearch
      , objectobjecttitle
      , objectobjecturl
      , path
      , referrer
      , search
      , title
      , url
      , user_id
      , id
      , timestamp
      , split_part(regexp_substr(context_page_search, 'utm_source=[A-Za-z0-9_]{0,100}'), '=', 2)  AS utm_source
      , split_part(regexp_substr(context_page_search, 'utm_medium=[A-Za-z0-9_]{0,100}'), '=', 2)  AS utm_medium
      , replace(replace(split_part(regexp_substr(context_page_search, 'utm_campaign=[A-Za-z0-9_%-]{0,100}'), '=', 2),
                        '%2B', ''), '%20', ' ')                                                   AS utm_campaign
      , split_part(regexp_substr(context_page_search, 'utm_adgroup=[A-Za-z0-9_]{0,100}'), '=', 2) AS utm_adgroup
      , split_part(regexp_substr(context_page_search, 'utm_content=[A-Za-z0-9_]{0,100}'), '=', 2) AS utm_content
      , replace(replace(split_part(regexp_substr(context_page_search, 'utm_term=[A-Za-z0-9_%-]{0,100}'), '=', 2), '%2B',
                        ''), '%20', ' ')                                                          AS utm_term
      , split_part(regexp_substr(context_page_search, 'gclid=[A-Za-z0-9_]{0,100}'), '=', 2)       AS gclid
      , split_part(replace(replace(replace(replace(split_part(
                                                           regexp_substr(context_page_referrer, 'q=[A-Za-z0-9_%+/:.-]{0,100}'),
                                                           '=', 2), '%2B', ''), '%20', ' '), '%2', ''), '+', ' '), ' %',
                   1)                                                                             AS referrer_search_term
      , CASE
            WHEN split_part(regexp_substr(context_page_referrer,
                                          '^(https://|http://)([a-z0-9-]{1,20}[\.]{1}|[A-Za-z0-9-]{1,100})[A-Za-z0-9]{0,100}[\.]{1}[a-z]{1,10}'),
                            '//', 2) IS NULL
                THEN 'Other'
                ELSE
                split_part(regexp_substr(context_page_referrer,
                                         '^(https://|http://)([a-z0-9-]{1,20}[\.]{1}|[A-Za-z0-9-]{1,100})[A-Za-z0-9-]{0,100}[\.]{1}[a-z]{1,10}'),
                           '//', 2) END                                                           AS referrer_source
    FROM {{ source('mattermostcom', 'pages') }}
    {% if is_incremental() %}

    WHERE timestamp > (SELECT MAX(timestamp) FROM {{this}})
    
    {% endif %}
    {{ dbt_utils.group_by(n=35) }}

    UNION ALL

    SELECT
        timestamp::DATE AS date
      , anonymous_id
      , channel
      , context_app_build
      , context_app_name
      , context_app_namespace
      , context_app_version
      , context_ip
      , context_library_name
      , context_library_version
      , context_locale
      , context_os_name
      , context_os_version
      , context_page_path
      , context_page_referrer
      , context_page_search
      , context_page_title
      , context_page_url
      , context_screen_density
      , context_traits_portal_customer_id
      , context_useragent
      , NULL            AS name
      , NULL            AS objectobjectpath
      , NULL            AS objectobjectreferrer
      , NULL            AS objectobjectsearch
      , NULL            AS objectobjecttitle
      , NULL            AS objectobjecturl
      , path
      , referrer
      , search
      , title
      , url
      , user_id
      , id
      , timestamp
      , split_part(regexp_substr(context_page_search, 'utm_source=[A-Za-z0-9_]{0,100}'), '=', 2)  AS utm_source
      , split_part(regexp_substr(context_page_search, 'utm_medium=[A-Za-z0-9_]{0,100}'), '=', 2)  AS utm_medium
      , replace(replace(split_part(regexp_substr(context_page_search, 'utm_campaign=[A-Za-z0-9_%-]{0,100}'), '=', 2),
                        '%2B', ''), '%20', ' ')                                                   AS utm_campaign
      , split_part(regexp_substr(context_page_search, 'utm_adgroup=[A-Za-z0-9_]{0,100}'), '=', 2) AS utm_adgroup
      , split_part(regexp_substr(context_page_search, 'utm_content=[A-Za-z0-9_]{0,100}'), '=', 2) AS utm_content
      , replace(replace(split_part(regexp_substr(context_page_search, 'utm_term=[A-Za-z0-9_%-]{0,100}'), '=', 2), '%2B',
                        ''), '%20', ' ')                                                          AS utm_term
      , split_part(regexp_substr(context_page_search, 'gclid=[A-Za-z0-9_]{0,100}'), '=', 2)       AS gclid
      , split_part(replace(replace(replace(replace(split_part(
                                                           regexp_substr(context_page_referrer, 'q=[A-Za-z0-9_%+/:.-]{0,100}'),
                                                           '=', 2), '%2B', ''), '%20', ' '), '%2', ''), '+', ' '), ' %',
                   1)                                                                             AS referrer_search_term
      , CASE
            WHEN split_part(regexp_substr(context_page_referrer,
                                          '^(https://|http://)([a-z0-9-]{1,20}[\.]{1}|[A-Za-z0-9-]{1,100})[A-Za-z0-9]{0,100}[\.]{1}[a-z]{1,10}'),
                            '//', 2) IS NULL
                THEN 'Other'
                ELSE
                split_part(regexp_substr(context_page_referrer,
                                         '^(https://|http://)([a-z0-9-]{1,20}[\.]{1}|[A-Za-z0-9-]{1,100})[A-Za-z0-9-]{0,100}[\.]{1}[a-z]{1,10}'),
                           '//', 2) END                                                           AS referrer_source
    FROM {{ source('portal_prod', 'pages') }}
    {% if is_incremental() %}

    WHERE timestamp > (SELECT MAX(timestamp) FROM {{this}})

    {% endif %}
    {{ dbt_utils.group_by(n=35) }}
)
SELECT *
FROM daily_website_traffic