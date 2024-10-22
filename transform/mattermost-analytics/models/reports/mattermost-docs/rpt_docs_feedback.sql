{{
    config({
        "materialized": "incremental",
        "incremental_strategy": "delete+insert",
        "unique_key": ['event_id'],
        "cluster_by": ['to_date(received_at)'],
        "snowflake_warehouse": "transform_l"
    })
}}
with feedback as (
    select
        event_id
        , timestamp
        , rating
        , feedback
        , label
        , received_at
        , context_ip as client_ip
        , parse_ip(context_ip, 'INET', 1) as parsed_ip
        , case
            when parsed_ip:error is null
                then parse_ip(context_ip || '/7', 'INET'):ipv4_range_start
            else null
        end as ip_bucket
        , case
            when context_user_agent is not null then mattermost_analytics.parse_user_agent(context_user_agent)
            else null
        end as parsed_user_agent
        , context_page_title as page_title
        , context_page_path as page_path
        , context_locale as browser_locale
        , context_page_search as page_search
        , context_campaign_source as utm_campaign_source
        , context_campaign_name as utm_campaign_name
        , context_campaign_medium as utm_campaign_medium
        , context_page_initial_referrer as initial_referrer
        , context_page_initial_referring_domain as initial_referring_domain
        , context_page_referrer as referrer
        , context_page_referring_domain as referring_domain


    from
        {{ ref('stg_mattermost_docs__feedback_submitted') }}
{% if is_incremental() %}
    where
        -- this filter will only be applied on an incremental run
       received_at >= (select max(received_at) from {{ this }})
{% endif %}
)
select
    f.event_id
    , f.timestamp
    , f.rating
    , f.feedback
    , f.label
    , f.received_at
    , f.client_ip
    , l.country_name as geolocated_country_name
    , f.browser_locale
    , f.page_path
    , f.page_title
    , f.page_search
    , f.parsed_user_agent:browser_family::varchar as ua_browser_family
    , f.parsed_user_agent:os_family::varchar as ua_os_family
    , f.parsed_user_agent:device_family::varchar as ua_device_family
    , f.utm_campaign_source
    , f.utm_campaign_name
    , f.utm_campaign_medium
    , f.initial_referrer
    , f.initial_referring_domain
    , f.referrer
    , f.referring_domain
from
    feedback f
    left join {{ ref('int_ip_country_lookup') }} l
                on f.ip_bucket = l.join_bucket
                    and f.parsed_ip:ipv4 between l.ipv4_range_start and l.ipv4_range_end