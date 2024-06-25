with source as (

    select * from {{ source('releases', 'log_entries') }}

),

renamed as (

    select
        to_date(logdate) as log_date
        , to_timestamp(logdate|| ' ' || logtime) as log_at
        , edge
        , bytessent
        , cip as viewer_ip
        , method
        , host
        , uri
        , status
        , creferrer
        , useragent
        , cs_uri_query
        , cookie
        , x_edge_result_type
        , x_edge_request_id
        , x_host_header
        , protocol
        , cs_bytes
        , time_taken
        , x_forwarded_for
        , ssl_protocol
        , ssl_cipher
        , x_edge_response_result_type
        , cs_protocol_version

        -- Derived fields
        , case
             when regexp_like(uri, '^\/[1-9]{1,3}[.][0-9]{1,3}[.][0-9]{1,3}\/.*') and uri like '%team%' then 'team'
             when regexp_like(uri, '^\/[1-9]{1,3}[.][0-9]{1,3}[.][0-9]{1,3}\/.*') and uri like '%enterprise%' then 'enterprise'
             when regexp_like(uri, '^\/[1-9]{1,3}[.][0-9]{1,3}[.][0-9]{1,3}\/.*') and not uri ilike any ('%team%', '%enterprise%') then 'enterprise'
             when regexp_like(uri, '^\/desktop\/[1-9]{1,3}[.][0-9]{1,3}[.][0-9]{1,3}\/.*') then 'desktop'
             else null
        end as download_type
        -- TODO: OS
        , case
            when regexp_like(uri, '^\/[1-9]{1,3}[.][0-9]{1,3}[.][0-9]{1,3}\/.*') then split_part(uri, '/', 2)
            when regexp_like(uri, '^\/desktop\/[1-9]{1,3}[.][0-9]{1,3}[.][0-9]{1,3}\/.*') then split_part(uri, '/', 3)
            else null
        end as version
        , split_part(version, '.', 1) as version_major
        , split_part(version, '.', 2) as version_minor
        , split_part(version, '.', 3) as version_patch

        -- Ignoring as always null
        -- , file_status
        -- , file_encrypted_fields

    from source

)

select * from renamed
