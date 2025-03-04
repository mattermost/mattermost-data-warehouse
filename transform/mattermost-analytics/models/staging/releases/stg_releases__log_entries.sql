with source as (

    select * from {{ source('releases', 'log_entries') }}

),

renamed as (

    select
        to_date(logdate) as log_date
        , to_timestamp(logdate|| ' ' || logtime) as log_at
        , edge
        , bytessent as response_bytes
        , cip as client_ip
        , method as http_method
        , host
        , uri
        , status
        , creferrer as referrer_url
        , replace(replace(useragent,'%2520', ' '), '%20', ' ') as user_agent
        , cs_uri_query as query_string
        , cookie
        , x_edge_result_type
        , x_edge_request_id as request_id
        , x_host_header
        , protocol
        , cs_bytes as request_bytes
        , time_taken
        , x_forwarded_for
        , ssl_protocol
        , ssl_cipher
        , x_edge_response_result_type
        , cs_protocol_version as http_protocol_version

        -- Derived fields
        , case
             when regexp_like(uri, '^\/[1-9]{1,3}[.][0-9]{1,3}[.][0-9]{1,3}\/.*') and uri like '%team%' then 'team'
             when regexp_like(uri, '^\/[1-9]{1,3}[.][0-9]{1,3}[.][0-9]{1,3}\/.*') and uri like '%enterprise%' then 'enterprise'
             when regexp_like(uri, '^\/[1-9]{1,3}[.][0-9]{1,3}[.][0-9]{1,3}\/.*') and not uri ilike any ('%team%', '%enterprise%') then 'enterprise'
             when regexp_like(uri, '^\/desktop\/[1-9]{1,3}[.][0-9]{1,3}[.][0-9]{1,3}\/.*') then 'desktop'
             else null
        end as download_type
        , case
            -- Desktop client
            when download_type = 'desktop' and uri like '%linux%' then 'linux'
            when download_type = 'desktop' and regexp_like(uri, '.*msi$|.*win[0-9]{2}.*$|.*\-win\-.*$|.*\-win\.[a-z]{3}$') then 'windows'
            when download_type = 'desktop' and regexp_like(uri, '.*mac\.[a-z]{3}$|.*\-osx\..*$') THEN 'mac'
            when download_type <> 'desktop' and uri like '%linux%' then 'linux'
            when download_type <> 'desktop' and regexp_like(uri, '.*\-windows\-.*$') THEN 'windows'
            else null
        end as operating_system
        , case
            when regexp_like(uri, '^\/[1-9]{1,3}[.][0-9]{1,3}[.][0-9]{1,3}\/.*') then split_part(uri, '/', 2)
            when regexp_like(uri, '^\/desktop\/[1-9]{1,3}[.][0-9]{1,3}[.][0-9]{1,3}\/.*') then split_part(uri, '/', 3)
            else null
        end as version
        , try_to_decimal(split_part(version, '.', 1)) as version_major
        , try_to_decimal(split_part(version, '.', 2)) as version_minor
        , try_to_decimal(split_part(version, '.', 3)) as version_patch

        -- User agent extracted
        , case when user_agent is not null then mattermost_analytics.parse_user_agent(user_agent) else null end as _parsed_user_agent
        , _parsed_user_agent:browser_family::varchar as ua_browser_family
        , _parsed_user_agent:os_family::varchar as ua_os_family

        -- Ignoring as always null
        -- , file_status
        -- , file_encrypted_fields

    from source
)

select * from renamed
