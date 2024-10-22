with feedback as (
    select
        id as event_id
        , timestamp
        , rating
        , feedback
        , label
        , received_at
        , context_ip as client_ip
        , parse_ip(context_ip, 'INET', 1) as parsed_ip
        , case
            when parsed_server_ip:error is null
                then parse_ip(context_ip || '/7', 'INET'):ipv4_range_start
            else null
        end as ip_bucket
        , context_page_path as page_path
        , context_locale as browser_locale
        , context_page_search as page_search
        , context_campaign_source as utm_campaign_source
        , context_campaign_name as utm_campaign_name
        , context_campaign_medium as utm_campaign_medium
    from
        {{ ref('stg_mattermost_docs__feedback_submitted') }}
)
select
    f.event_id
    , f.timestamp
    , f.rating
    , f.feedback
    , f.label
    , f.received_at
    , f.client_ip
    , l.ip_country_name
    , f.page_path
    , f.browser_locale
    , f.page_search
    , f.utm_campaign_source
    , f.utm_campaign_name
    , f.utm_campaign_medium
from
    feedback f
    left join {{ ref('int_ip_country_lookup') }} l
                on f.ip_bucket = l.join_bucket
                    and f..parsed_ip:ipv4 between l.ipv4_range_start and l.ipv4_range_end